"""The weekly suite: small file sets, every tool once on a file set, and nothing measured twice."""

import json
import math
from pathlib import Path
import unittest

from benchmarks import matrix
from . import common, production_diagnostics as diagnostics, production_projection as projection
from . import production_workflow as workflow, test_matrix_diagnostics, test_matrix_results, worker
from .test_matrix_results import matrix_record
from .test_storage_cells import digest

ROOT = Path(__file__).resolve().parents[2]
WEEKLY = "matrix-weekly-2"
# the suite defined before it, which never ran
FIRST = "matrix-weekly-1"
# the sets of large files, the set of small files, and the sets of small files on other storage
LARGE = ("bulk-1g-200gb", "bulk-1m-200gb")
SMALL, NFS_SET, MEMORY_SET = "tiny-1k-2048k", "tiny-1k-700x1024", "tiny-1k-2048k-memory"
TOOLS = ("rcmp", "rlink", "rrm")
LOCAL, REMOTE, NFS, MEMORY = (
    (copies, *(copies + "-" + tool for tool in TOOLS)) for copies in ("local", "remote", "nfs", "memory"))
# the cells of the small files on the arrays: every tool after the local copies, and after the remote ones
# only the tool that removes them
ARRAYS = (*LOCAL, "remote", "remote-rrm")
# what the run of 25 November 2026 measured on the sets before these (repo-655073755-run-45515239343):
# the slowest median of each kind of command, in seconds, and the bytes or files it worked on
MEASURED_COPY_SECONDS_PER_BYTE = {"bulk-1g-200gb": 622.1 / 1000e9, "bulk-1m-200gb": 644.2 / 1024e9}
MEASURED_SECONDS_PER_FILE = {"local": 649.5 / 10.24e6, "remote": 204.8 / 10.24e6,
                             "local-rcmp": 656.7 / 10.24e6, "local-rlink": 577.5 / 10.24e6,
                             "local-rrm": 296.7 / 10.24e6, "remote-rrm": 339.5 / 10.24e6}


def manifest_cases():
    return {case["id"]: case for case in json.loads((ROOT / "benchmarks/matrix-cases.json").read_text())["cases"]}


def files(case):
    return math.prod(case["directory_widths"]) * case["files_per_leaf"]


class WeeklySuiteTests(unittest.TestCase):
    def test_copies_of_every_file_set_and_each_tool_once_on_a_set_of_small_files(self):
        cells = common.MATRIX_SUITES[WEEKLY]
        self.assertEqual(cells, (
            *((case, scenario) for case in LARGE for scenario in ("local", "remote")),
            *((SMALL, scenario) for scenario in ARRAYS),
            *((NFS_SET, scenario) for scenario in NFS), *((MEMORY_SET, scenario) for scenario in MEMORY)))
        self.assertEqual(len(cells), 18)
        self.assertEqual(common.SUITES[WEEKLY], (*LARGE, SMALL, NFS_SET, MEMORY_SET))
        # a tool takes as long as its files are many, whatever their size: on a set of large files
        # it is done in a second or two, which measures nothing
        cases = manifest_cases()
        for case, scenario in cells:
            if scenario in common.MATRIX_TOOL_CELLS:
                self.assertEqual(cases[case]["file_size_bytes"], 1000, (case, scenario))
        selected = matrix.selection(matrix.run.load_manifest(ROOT / "benchmarks/matrix-cases.json"),
                                    [case + ":" + scenario for case, scenario in cells])
        self.assertEqual([(case["id"], scenario) for case, scenario in selected], list(cells))

    def test_a_tool_that_leaves_the_copy_as_it_was_runs_once_on_a_file_set(self):
        # a remote copy leaves the tree that a local copy leaves, so comparing it and hard-linking it once
        # more measures the same again: in the run of 25 November those second runs took 89 minutes and
        # read 3.5% slower than the first. The copy has to go either way, and rrm is what removes it
        cells = common.MATRIX_SUITES[WEEKLY]
        for tool in ("rcmp", "rlink"):
            self.assertEqual([cell for cell in cells if cell[1].endswith("-" + tool)],
                             [(SMALL, "local-" + tool), (NFS_SET, "nfs-" + tool), (MEMORY_SET, "memory-" + tool)])
        self.assertEqual([cell for cell in cells if cell[1].endswith("-rrm")],
                         [(SMALL, "local-rrm"), (SMALL, "remote-rrm"), (NFS_SET, "nfs-rrm"),
                          (MEMORY_SET, "memory-rrm")])
        # no two file sets with the same files on the same storage: the set of 100 MB files gave what the set
        # of 1 GB files gave (639 and 621 s, 941 and 896 s of CPU)
        cases = manifest_cases()
        sizes = [cases[case]["file_size_bytes"] for case in common.SUITES[WEEKLY]]
        self.assertEqual(sizes, [1_000_000_000, 1_000_000, 1000, 1000, 1000])

    def test_it_follows_the_suites_there_were(self):
        self.assertEqual(common.DURABLE_SUITES[47:49], (FIRST, WEEKLY))
        # the suites with reference cells follow (test_reference_cells.py)
        self.assertEqual(len(common.MATRIX_SUITES), 46)
        self.assertEqual(common.MATRIX_WEEKLY, WEEKLY)
        # its cells never change: another choice of file sets or scenarios is another suite, with the next number
        self.assertRegex(WEEKLY, r"^matrix-weekly-[1-9][0-9]*$")
        # what the schedule runs is changed in a step of its own, after this suite has run by hand
        self.assertEqual(workflow.SCHEDULED_SUITE, "matrix-all-local+remote")

    def test_the_suite_before_it_stays_as_it_was_recorded(self):
        # it never ran, and a suite is not changed for that: its name, its cells and its plan are what they were
        large = ("bulk-1g-400gb", "bulk-100m-400gb", "bulk-1m-400gb")
        self.assertEqual(common.MATRIX_SUITES[FIRST], (
            *((case, scenario) for case in large for scenario in ("local", "remote")),
            *(("tiny-1k-4096k", scenario) for scenario in (*LOCAL, *REMOTE)),
            *((NFS_SET, scenario) for scenario in NFS), *(("tiny-1k-4096k-memory", scenario) for scenario in MEMORY)))
        # everything the code records of it, as it was merged
        self.assertEqual(digest([common.MATRIX_SUITES[FIRST], common.durable_plan_for_suite(FIRST),
                                 common.durable_plan_contract(common.durable_plan_for_suite(FIRST)),
                                 common.matrix_contract(FIRST), common.SUITES[FIRST], common.SUITE_PROFILES[FIRST]]),
                         "b9e6b236c61ba0f97c604038c3aa39860ac382b163b94fa6aa754bd4e0f924cd")
        first, weekly = (common.durable_plan_contract(common.durable_plan_for_suite(suite))
                         for suite in (FIRST, WEEKLY))
        self.assertEqual(first["plan"]["version"], FIRST + "-plan-v1")
        skipped = ("suite", "plan")
        self.assertEqual({key: first[key] for key in first if key not in skipped},
                         {key: weekly[key] for key in weekly if key not in skipped})
        self.assertEqual(first["plan"], dict(weekly["plan"], version=FIRST + "-plan-v1"))
        cases = manifest_cases()
        self.assertEqual({name: files(cases[name]) for name in (*large, "tiny-1k-4096k", "tiny-1k-4096k-memory")},
                         {"bulk-1g-400gb": 400, "bulk-100m-400gb": 4000, "bulk-1m-400gb": 400_000,
                          "tiny-1k-4096k": 4_096_000, "tiny-1k-4096k-memory": 4_096_000})

    def test_the_file_sets_are_the_first_ones_made_five_times_smaller(self):
        cases = manifest_cases()
        shape = lambda name: (cases[name]["directory_widths"], cases[name]["files_per_leaf"],
                              cases[name]["file_size_bytes"])
        self.assertEqual({name: shape(name) for name in (*LARGE, SMALL, MEMORY_SET)}, {
            "bulk-1g-200gb": ([10], 20, 1_000_000_000), "bulk-1m-200gb": ([10, 10, 10], 200, 1_000_000),
            SMALL: ([2, 10, 10, 10], 1024, 1000), MEMORY_SET: ([2, 10, 10, 10], 1024, 1000)})
        for name in LARGE:
            self.assertEqual(files(cases[name]) * cases[name]["file_size_bytes"], 200_000_000_000)
        self.assertEqual(files(cases[SMALL]), 2_048_000)
        self.assertEqual(files(cases["tiny-1k-10240k"]), 5 * files(cases[SMALL]))
        # the directories of each set are those of the set it replaces, fewer of them or less full
        for name, before in (("bulk-1g-200gb", "bulk-1g-1tb"), ("bulk-1m-200gb", "bulk-1m-1024gb")):
            self.assertEqual(cases[name]["directory_widths"], cases[before]["directory_widths"])
            self.assertEqual(cases[name]["file_size_bytes"], cases[before]["file_size_bytes"])
        self.assertEqual(cases[SMALL]["files_per_leaf"], cases["tiny-1k-10240k"]["files_per_leaf"])
        for name in (*LARGE, SMALL, MEMORY_SET):
            self.assertEqual(diagnostics.MATRIX_CASE_SHAPES[name], dict(
                directory_widths=cases[name]["directory_widths"], files_per_leaf=cases[name]["files_per_leaf"],
                file_size_bytes=cases[name]["file_size_bytes"]))

    def test_every_command_on_the_arrays_is_expected_to_take_a_minute_or_two(self):
        # a copy of large files takes as long as their bytes take to write, and everything on
        # small files as long as their number: in proportion, the rates of the run before give these
        # times. A copy of large files may be shorter than its share: the copies of a terabyte ended
        # with some tens of gigabytes still to be written, and that is a larger part of 200 GB
        cases, expected = manifest_cases(), {}
        for case, scenario in common.MATRIX_SUITES[WEEKLY]:
            if case in LARGE:
                size = files(cases[case]) * cases[case]["file_size_bytes"]
                expected[case, scenario] = MEASURED_COPY_SECONDS_PER_BYTE[case] * size
            elif case == SMALL:
                expected[case, scenario] = MEASURED_SECONDS_PER_FILE[scenario] * files(cases[case])
        self.assertEqual(len(expected), 10)
        for cell, seconds in expected.items():
            with self.subTest(cell=cell):
                self.assertGreaterEqual(seconds, 40, cell)
                self.assertLessEqual(seconds, 135, cell)
        self.assertEqual({cell: round(seconds) for cell, seconds in expected.items() if cell[0] != SMALL},
                         {("bulk-1g-200gb", "local"): 124, ("bulk-1g-200gb", "remote"): 124,
                          ("bulk-1m-200gb", "local"): 126, ("bulk-1m-200gb", "remote"): 126})
        self.assertEqual({scenario: round(expected[SMALL, scenario]) for scenario in ARRAYS},
                         {"local": 130, "local-rcmp": 131, "local-rlink": 116, "local-rrm": 59,
                          "remote": 41, "remote-rrm": 68})

    def test_it_has_eight_hours_and_sends_the_tools(self):
        contract = common.durable_plan_contract(common.durable_plan_for_suite(WEEKLY))
        arrays = common.durable_plan_contract(common.durable_plan_for_suite("matrix-all-local+remote"))
        # about two hours of work are expected, a third of them on storage that no worker has measured
        # yet: the eight-hour allocation that the copies of every file set had, and its reservation. The
        # bill follows the time a run takes
        self.assertEqual((contract["max_runtime"], contract["runtime_profile"], contract["reservation_microusd"]),
                         (28800, "all-filesets-8h-v1", 50_000_000))
        skipped = ("suite", "plan", "binary_names", "binaries")
        self.assertEqual({key: contract[key] for key in contract if key not in skipped},
                         {key: arrays[key] for key in arrays if key not in skipped})
        self.assertEqual(contract["plan"], dict(arrays["plan"], version=WEEKLY + "-plan-v1"))
        self.assertEqual(contract["binary_names"], ("rcp", "rcpd", "filegen", *TOOLS))
        self.assertEqual(common.matrix_contract(WEEKLY), dict(common.matrix_contract("matrix-all-local"),
                                                              cells=common.MATRIX_SUITES[WEEKLY]))
        self.assertFalse(common.SUITE_PROFILES[WEEKLY]["active"])

    def test_the_worker_prepares_every_storage_and_fits_the_sets_of_the_arrays(self):
        argv = worker.command(dict(suite=WEEKLY, baseline="a" * 40), 9000,
                              dict(nfs=dict(layout="nfs-loopback-v1"), memory=dict(layout="tmpfs-pair-v1")))
        args = matrix._arguments(argv[argv.index("benchmarks.matrix") + 1:])
        self.assertEqual(args.cells, [case + ":" + scenario for case, scenario in common.MATRIX_SUITES[WEEKLY]])
        self.assertEqual((args.nfs_source_root.name, args.memory_destination_root.name),
                         ("nfs-source", "memory-destination"))
        # four file sets are kept on the arrays; the one in memory is not theirs to fit
        kept = [case for case in common.SUITES[WEEKLY] if any(
            common.matrix_storage(scenario) != "memory" for name, scenario in common.MATRIX_SUITES[WEEKLY]
            if name == case)]
        self.assertEqual(kept, [*LARGE, SMALL, NFS_SET])

    def test_a_run_is_collected_and_published_as_the_runs_with_cells_on_other_storage(self):
        cells = common.MATRIX_SUITES[WEEKLY]
        self.assertEqual(projection.matrix_schema(scenario for _, scenario in cells),
                         "rcp-production-public-report-v6")
        collect = test_matrix_results.MatrixCollectionTests.collect
        result, report, public = collect(self, WEEKLY, matrix_record(WEEKLY), remaining=28300,
                                         runtime_seconds=28800, capacity_seconds=7200)
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        self.assertEqual((len(public["cells"]), len(public["measurements"])), (18, 18))
        record = projection.bind(public, "a" * 64)
        raw = matrix_record(WEEKLY, environment=test_matrix_diagnostics.ENVIRONMENT)
        raw["timestamp"] = record["timestamp"]
        extra = diagnostics.derive(record, b"archive", raw, test_matrix_diagnostics.WORKER)
        self.assertEqual((extra["schema"], extra["status"], len(extra["trials"]), len(extra["cases"])),
                         ("rcp-public-resource-diagnostics-v5", "complete", 54, 5))


if __name__ == "__main__":
    unittest.main()
