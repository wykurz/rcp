"""Cells on NFS mounts and on roots in memory: the suites that select them and what the worker gives the runner."""

import contextlib
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import MappingProxyType
import unittest
from unittest import mock

from benchmarks import matrix, telemetry
from . import common, other_storage, production_collection as collection, production_diagnostics as diagnostics
from . import production_export as export, production_history as history
from . import production_projection as projection, storage_check, test_matrix_diagnostics, test_matrix_results, worker
from .test_durable_worker import durable_fixture, request as worker_request
from .test_matrix_pages import matrix_example
from .test_matrix_results import matrix_record
from .test_production_history import split
from .test_tool_cells import sampled_matrix
from .test_tool_pages import weeks
from .test_worker_review import report

ROOT = Path(__file__).resolve().parents[2]
FILE_SETS = ("bulk-1g-1tb", "bulk-100m-1024gb", "bulk-1m-1024gb", "tiny-1k-10240k")
# what the cells on a network filesystem and the cells in memory measure
NFS_SET, MEMORY_SET = "tiny-1k-700x1024", "tiny-1k-10240k-memory"
TOOLS = ("rcmp", "rlink", "rrm")
LOCAL, REMOTE, NFS, MEMORY = (
    (copies, *(copies + "-" + tool for tool in TOOLS)) for copies in ("local", "remote", "nfs", "memory"))
EVERYWHERE = "local+tools+remote+tools+nfs+tools+memory+tools"
# the suites this release adds, in the order they are appended
ADDED = tuple(f"matrix-{selection}-{scenarios}" for scenarios in ("nfs+tools", "memory+tools", EVERYWHERE)
              for selection in ("all", "commission"))
WEEKLY = "matrix-all-" + EVERYWHERE


def plain(value):
    if isinstance(value, (dict, MappingProxyType)):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    return value


def digest(value):
    return hashlib.sha256(json.dumps(plain(value), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class StorageSuiteTests(unittest.TestCase):
    def test_the_tools_follow_copies_on_every_storage(self):
        self.assertEqual(common.MATRIX_TOOL_CELLS, frozenset(matrix.TOOLS))
        self.assertEqual(common.MATRIX_TOOL_CELLS, frozenset((*LOCAL[1:], *REMOTE[1:], *NFS[1:], *MEMORY[1:])))

    def test_three_kinds_of_suite_measure_on_other_storage(self):
        self.assertEqual(common.DURABLE_SUITES[41:47], ADDED)
        suites = common.MATRIX_SUITES
        # the file set of each storage alone, for a short run after a change that matters there
        self.assertEqual(suites["matrix-all-nfs+tools"], tuple((NFS_SET, scenario) for scenario in NFS))
        self.assertEqual(suites["matrix-all-memory+tools"], tuple((MEMORY_SET, scenario) for scenario in MEMORY))
        # and everything: each file set under the scenarios of the storage it is kept on
        self.assertEqual(suites[WEEKLY], (
            *((case, scenario) for case in FILE_SETS for scenario in (*LOCAL, *REMOTE)),
            *((NFS_SET, scenario) for scenario in NFS), *((MEMORY_SET, scenario) for scenario in MEMORY)))
        self.assertEqual(len(suites[WEEKLY]), 40)
        self.assertEqual(common.SUITES[WEEKLY], (*FILE_SETS, NFS_SET, MEMORY_SET))
        # commissioning: the small fixture of the disks serves the mounts too, since they show the
        # same trees; the roots in memory have a fixture of their own
        self.assertEqual(suites["matrix-commission-nfs+tools"], tuple(("commission-10k", scenario) for scenario in NFS))
        self.assertEqual(suites["matrix-commission-memory+tools"],
                         tuple(("commission-10k-memory", scenario) for scenario in MEMORY))
        self.assertEqual(suites["matrix-commission-" + EVERYWHERE], (
            *(("commission-64m", scenario) for scenario in (*LOCAL, *REMOTE)),
            *(("commission-10k", scenario) for scenario in (*LOCAL, *REMOTE, *NFS)),
            *(("commission-10k-memory", scenario) for scenario in MEMORY)))
        self.assertEqual(common.SUITES["matrix-commission-" + EVERYWHERE],
                         ("commission-64m", "commission-10k", "commission-10k-memory"))

    def test_the_runner_accepts_every_selection(self):
        manifest = matrix.run.load_manifest(ROOT / "benchmarks/matrix-cases.json")
        for suite in ADDED:
            with self.subTest(suite=suite):
                cells = common.MATRIX_SUITES[suite]
                selected = matrix.selection(manifest, [case + ":" + scenario for case, scenario in cells])
                self.assertEqual([(case["id"], scenario) for case, scenario in selected], list(cells))
                self.assertEqual(common.SUITES[suite], tuple(dict.fromkeys(case for case, _ in cells)))

    def test_a_single_file_set_has_no_suite_on_other_storage(self):
        # the sets of large files are not what a network filesystem or memory is measured with
        for selection in ("1g", "100m", "1m", "1k"):
            for scenarios in ("nfs+tools", "memory+tools", EVERYWHERE):
                self.assertNotIn(f"matrix-{selection}-{scenarios}", common.SUITE_PROFILES)

    def test_the_file_sets_fit_their_storage(self):
        manifest = json.loads((ROOT / "benchmarks/matrix-cases.json").read_text())
        cases = {case["id"]: case for case in manifest["cases"]}
        # on a network filesystem a directory takes one new file at a time: 700 directories, each
        # of 1,024 files of 1 KB, are five minutes of copying at five milliseconds a request
        self.assertEqual({key: cases[NFS_SET][key] for key in ("directory_widths", "files_per_leaf", "file_size_bytes")},
                         dict(directory_widths=[7, 10, 10], files_per_leaf=1024, file_size_bytes=1000))
        # in memory: the tiny files of the disks, under a name of their own because a file set has one home
        shape = ("directory_widths", "files_per_leaf", "file_size_bytes")
        self.assertEqual([cases[MEMORY_SET][key] for key in shape], [cases["tiny-1k-10240k"][key] for key in shape])
        self.assertEqual([cases["commission-10k-memory"][key] for key in shape],
                         [cases["commission-10k"][key] for key in shape])

    def test_a_root_in_memory_holds_a_copy_of_its_file_set_and_the_links_rlink_makes_to_it(self):
        manifest = matrix.run.load_manifest(ROOT / "benchmarks/matrix-cases.json")
        for case in (item for item in manifest["cases"] if item["id"] in (MEMORY_SET, "commission-10k-memory")):
            counts = matrix.operations.expected_counts(case)
            with self.subTest(case=case["id"]):
                # on a filesystem in memory a hard link takes one of the limit's inodes, as a file
                # does: the destination root holds the copy and the tree that rlink makes of it
                self.assertGreaterEqual(other_storage.MEMORY_INODES, 2 * (counts["files"] + counts["directories"]))
                # and every file takes at least a page, however small it is
                pages = counts["files"] * max(4096, -(-case["file_size_bytes"] // 4096) * 4096)
                self.assertGreaterEqual(other_storage.MEMORY_BYTES, pages * 5 // 4)
        # two roots, and the machine has 192 GiB of memory: the limits leave it room to work
        self.assertLessEqual(2 * other_storage.MEMORY_BYTES, 128 * 1024 ** 3)

    def test_earlier_suites_and_every_recorded_plan_are_what_they_were(self):
        # a plan version is a promise to every run recorded under it
        later = (*ADDED, *common.MATRIX_WEEKLY_SUITES, *common.MATRIX_REFERENCE_SUITES)
        earlier = {version: contract for version, contract in common._RECORDED_DURABLE_CONTRACTS.items()
                   if not any(version.startswith(suite + "-plan-") for suite in later)}
        self.assertEqual(len(earlier), 44)
        self.assertEqual(digest(earlier), "ac0f65f28501449ee99480328c013ba12197a404b60495edeb9e6fd961d17f67")
        suites = {suite: [common.MATRIX_SUITES[suite], common.durable_plan_for_suite(suite),
                          common.matrix_contract(suite)] for suite in common.MATRIX_SUITES if suite not in later}
        self.assertEqual(len(suites), 36)
        self.assertEqual(digest([list(common.DURABLE_SUITES[:41]), suites]),
                         "2d110ef7a2849d92eb19524e1897c743c7f970a3417cd8f271b2abd1c2c00bd3")

    def test_everything_has_twelve_hours_and_the_rest_the_ordinary_allocation(self):
        tools = common.durable_plan_contract(common.durable_plan_for_suite("matrix-all-local+tools+remote+tools"))
        weekly = common.durable_plan_contract(common.durable_plan_for_suite(WEEKLY))
        skipped = ("suite", "plan")
        self.assertEqual({key: weekly[key] for key in weekly if key not in skipped},
                         {key: tools[key] for key in tools if key not in skipped})
        self.assertEqual((weekly["max_runtime"], weekly["runtime_profile"], weekly["reservation_microusd"]),
                         (43200, "all-filesets-12h-v1", 65_000_000))
        # a new suite starts at its first version: there was never an eight-hour one to keep
        self.assertEqual(weekly["plan"], dict(tools["plan"], version=WEEKLY + "-plan-v1"))
        self.assertNotIn(WEEKLY + "-plan-v2", common._RECORDED_DURABLE_CONTRACTS)
        ordinary = common.durable_plan_contract(common.durable_plan_for_suite("matrix-1k-local+tools"))
        for suite in ADDED:
            if suite == WEEKLY:
                continue
            with self.subTest(suite=suite):
                contract = common.durable_plan_contract(common.durable_plan_for_suite(suite))
                self.assertEqual({key: contract[key] for key in contract if key not in skipped},
                                 {key: ordinary[key] for key in ordinary if key not in skipped})
                self.assertEqual(contract["plan"], dict(ordinary["plan"], version=suite + "-plan-v1"))
                self.assertEqual((contract["max_runtime"], contract.get("reservation_microusd")), (10800, None))
        versions = [common.durable_plan_for_suite(suite)["version"] for suite in common.DURABLE_SUITES]
        self.assertEqual(len(set(versions)), len(versions))

    def test_the_new_suites_measure_as_the_others_do_and_send_the_tools(self):
        for suite in ADDED:
            with self.subTest(suite=suite):
                contract = common.matrix_contract(suite)
                self.assertEqual(contract, dict(common.matrix_contract("matrix-all-local"),
                                                cells=common.MATRIX_SUITES[suite],
                                                purpose="commissioning" if "commission" in suite else "performance"))
                self.assertEqual(common.durable_plan_contract(common.durable_plan_for_suite(suite))["binary_names"],
                                 ("rcp", "rcpd", "filegen", *TOOLS))
                self.assertFalse(common.SUITE_PROFILES[suite]["active"])


def properties(argv):
    return dict(item[len("--property="):].split("=", 1) for item in argv if item.startswith("--property="))


COMMISSION = "matrix-commission-" + EVERYWHERE
NFS_IDS = dict(source="c5d-raid0-ext4-source-nfs42-5ms-v1", destination="c5d-raid0-ext4-destination-nfs42-5ms-v1")
MEMORY_IDS = dict(source="c5d-tmpfs-source-v1", destination="c5d-tmpfs-destination-v1")


class StorageWorkerTests(unittest.TestCase):
    """The supervisor prepares the mounts and the roots, and the runner is told where they are or why not."""

    def arguments(self, suite, other):
        argv = worker.command(dict(suite=suite, baseline="a" * 40), 9000, other)
        return matrix._arguments(argv[argv.index("benchmarks.matrix") + 1:]), argv

    def test_the_runner_is_given_the_mounts_and_roots_the_worker_prepared(self):
        args, argv = self.arguments(COMMISSION, dict(nfs=dict(layout="nfs-loopback-v1"),
                                                     memory=dict(layout="tmpfs-pair-v1")))
        self.assertEqual(args.cells, [case + ":" + scenario for case, scenario in common.MATRIX_SUITES[COMMISSION]])
        self.assertEqual((args.nfs_source_root, args.nfs_destination_root),
                         (worker.DATA / "nfs-source", worker.DATA / "nfs-destination"))
        self.assertEqual((args.memory_source_root, args.memory_destination_root),
                         (worker.DATA / "memory-source", worker.DATA / "memory-destination"))
        self.assertEqual(dict(source=args.nfs_source_storage_id, destination=args.nfs_destination_storage_id), NFS_IDS)
        self.assertEqual(dict(source=args.memory_source_storage_id, destination=args.memory_destination_storage_id),
                         MEMORY_IDS)
        self.assertEqual((args.nfs_unavailable, args.memory_unavailable), (None, None))
        # the arrays are the roots of the run as they always were
        self.assertEqual((args.source_root, args.source_storage_id), (worker.DATA / "source", "c5d-raid0-ext4-source-v1"))
        # no cell on other storage opens a socket: the unit is the one of the suite without them
        self.assertEqual(properties(argv), properties(worker.command(
            dict(suite="matrix-commission-local+tools+remote+tools", baseline="a" * 40), 9000)))
        alone = properties(worker.command(dict(suite="matrix-commission-nfs+tools", baseline="a" * 40), 9000,
                                          dict(nfs=dict(layout="nfs-loopback-v1"))))
        self.assertEqual(alone["RestrictAddressFamilies"], "AF_UNIX")

    def test_the_name_of_a_storage_says_what_it_is(self):
        # a result is kept on the line of its storage: another protocol version or another delay
        # is other storage and gets another name
        self.assertEqual((other_storage.NFS_VERSION, 2 * other_storage.NFS_DELAY_MICROSECONDS), ("4.2", 5000))
        self.assertEqual(other_storage.STORAGE_IDS, dict(nfs=NFS_IDS, memory=MEMORY_IDS))
        self.assertEqual(set(other_storage.STORAGE_IDS), set(matrix.STORAGES))

    def test_storage_the_worker_could_not_prepare_is_named_with_the_reason(self):
        failed = dict(nfs=dict(error=dict(type="CalledProcessError", command="modprobe", returncode=1)),
                      memory=dict(error=dict(type="ValidationError", message="memory root already exists")))
        args, _ = self.arguments(COMMISSION, failed)
        self.assertEqual(args.nfs_unavailable, "the worker could not prepare this storage: modprobe exited with status 1")
        self.assertEqual(args.memory_unavailable,
                         "the worker could not prepare this storage: memory root already exists")
        self.assertEqual((args.nfs_source_root, args.nfs_destination_root, args.memory_source_root), (None,) * 3)
        # the cells keep their names for the storage, and with them their identity
        self.assertEqual(dict(source=args.nfs_source_storage_id, destination=args.nfs_destination_storage_id), NFS_IDS)
        # the runner takes these arguments: every other cell is measured
        self.assertEqual(sorted(matrix.storages([(dict(id=case), scenario) for case, scenario
                                                 in common.MATRIX_SUITES[COMMISSION]], args)), ["memory", "nfs"])
        for other, expected in (({}, "the worker did not prepare this storage"),
                                (dict(nfs=dict(error=dict(type="TimeoutExpired", command="mount.nfs", timeout=60.0))),
                                 "the worker could not prepare this storage: mount.nfs did not finish"),
                                (dict(nfs=dict(error=dict(type="OSError", code="ENOSPC"))),
                                 "the worker could not prepare this storage: OSError")):
            with self.subTest(other=other):
                self.assertEqual(self.arguments("matrix-commission-nfs+tools", other)[0].nfs_unavailable, expected)

    def test_a_suite_of_the_arrays_alone_is_started_as_before(self):
        for other in (None, {}, dict(nfs=dict(layout="nfs-loopback-v1"))):
            argv = worker.command(dict(suite="matrix-commission-local+tools+remote+tools", baseline="a" * 40), 9000,
                                  *(() if other is None else (other,)))
            self.assertEqual([word for word in argv if word.startswith(("--nfs-", "--memory-"))], [])

    def test_the_worker_prepares_only_the_storage_its_suite_measures_on(self):
        budget = object()
        for suite, kinds in ((COMMISSION, ["nfs", "memory"]), ("matrix-all-nfs+tools", ["nfs"]),
                             ("matrix-all-memory+tools", ["memory"]), ("matrix-all-local+tools+remote+tools", [])):
            with self.subTest(suite=suite), \
                    mock.patch.object(other_storage, "prepare_nfs", return_value=dict(layout="a")) as nfs, \
                    mock.patch.object(other_storage, "prepare_memory", return_value=dict(layout="b")) as memory:
                prepared = worker.prepare_other_storage(common.MATRIX_SUITES[suite], budget, 123, 456)
                self.assertEqual(list(prepared), kinds)
                self.assertEqual((nfs.call_count, memory.call_count), ("nfs" in kinds, "memory" in kinds))
                if "nfs" in kinds:
                    nfs.assert_called_once_with(worker.DATA, worker.STATE, budget, worker.execute)
                if "memory" in kinds:
                    memory.assert_called_once_with(worker.DATA, budget, 123, 456, worker.execute)

    def test_storage_that_cannot_be_prepared_costs_only_its_own_cells(self):
        error = subprocess.CalledProcessError(1, ["modprobe", "--all", "sch_netem"])
        with mock.patch.object(other_storage, "prepare_nfs", side_effect=error), \
                mock.patch.object(other_storage, "prepare_memory", return_value=dict(layout="b")):
            prepared = worker.prepare_other_storage(common.MATRIX_SUITES[COMMISSION], object(), 1, 2)
        self.assertEqual(prepared, dict(nfs=dict(error=dict(type="CalledProcessError", command="modprobe", returncode=1)),
                                        memory=dict(layout="b")))
        # the end of the allocation is not a failure of the storage, and ends the run as it always does
        with mock.patch.object(other_storage, "prepare_nfs", side_effect=worker.DeadlineExpired()), \
                self.assertRaises(worker.DeadlineExpired):
            worker.prepare_other_storage(common.MATRIX_SUITES[COMMISSION], object(), 1, 2)

    def worker_run(self, suite, **patches):
        """Run the worker for a suite with these stand-ins for the storage preparation; return what it reported."""
        with durable_fixture() as (root, cloud, measure, _, _, prepare):
            lease = json.loads(cloud.objects[common.WINDOW_KEY])
            lease.update(suite=suite, plan=common.durable_plan_for_suite(suite))
            request = worker_request(lease)
            request["sha256"] = dict.fromkeys(common.suite_binaries(suite), common.digest(b"binary"))
            cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
            cloud.objects[common.REQUEST_KEY] = json.dumps(request).encode()
            cloud.objects.update({"input/" + key: b"binary" for key in common.suite_binaries(suite)})
            (root / "benchmarks/matrix-cases.json").write_bytes((ROOT / "benchmarks/matrix-cases.json").read_bytes())
            with mock.patch.multiple(other_storage, **patches):
                worker.run(cloud.api["s3"], "bucket", direct=True)
            metadata, _ = report(cloud)
            return metadata, measure, prepare

    def test_a_run_prepares_the_other_storage_after_the_arrays_and_reports_it(self):
        nfs = mock.Mock(return_value=dict(layout="nfs-loopback-v1", round_trip_seconds=0.0052))
        memory = mock.Mock(side_effect=ValueError("memory root already exists"))
        metadata, measure, prepare = self.worker_run(COMMISSION, prepare_nfs=nfs, prepare_memory=memory)
        self.assertTrue(metadata["status"]["success"], metadata["status"])
        self.assertEqual(metadata["other_storage"], dict(
            nfs=dict(layout="nfs-loopback-v1", round_trip_seconds=0.0052),
            memory=dict(error=dict(type="ValueError", message="memory root already exists"))))
        argv = measure.call_args.args[0]
        args = matrix._arguments(argv[argv.index("benchmarks.matrix") + 1:])
        # under the data directory of this run, beside the arrays
        self.assertEqual((args.nfs_source_root, args.nfs_destination_root),
                         (args.source_root.parent / "nfs-source", args.source_root.parent / "nfs-destination"))
        self.assertEqual(args.memory_unavailable, "the worker could not prepare this storage: memory root already exists")
        # the arrays hold what is kept on them: the fixture in memory is not theirs to fit
        self.assertEqual([case["id"] for case in prepare.call_args.args[1]], ["commission-64m", "commission-10k"])

    def test_a_run_on_the_arrays_alone_prepares_nothing_else(self):
        nfs, memory = mock.Mock(), mock.Mock()
        metadata, _, prepare = self.worker_run("matrix-commission-local+tools", prepare_nfs=nfs, prepare_memory=memory)
        self.assertTrue(metadata["status"]["success"], metadata["status"])
        self.assertNotIn("other_storage", metadata)
        self.assertEqual((nfs.call_count, memory.call_count), (0, 0))
        self.assertEqual([case["id"] for case in prepare.call_args.args[1]], ["commission-64m", "commission-10k"])

    def test_a_run_in_memory_alone_still_has_its_arrays(self):
        memory = mock.Mock(return_value=dict(layout="tmpfs-pair-v1"))
        metadata, measure, prepare = self.worker_run("matrix-commission-memory+tools", prepare_nfs=mock.Mock(),
                                                     prepare_memory=memory)
        self.assertTrue(metadata["status"]["success"], metadata["status"])
        # the runner keeps its scratch directories and its results beside them
        self.assertEqual(prepare.call_args.args[1], ())
        self.assertEqual(metadata["other_storage"], dict(memory=dict(layout="tmpfs-pair-v1")))


V4, V5, V6 = (f"rcp-production-public-report-v{number}" for number in (4, 5, 6))
MISSING = "the worker could not prepare this storage: modprobe exited with status 1"


class StorageRecordTests(unittest.TestCase):
    """The collector and the public record take the cells on other storage as they take the others."""

    collect = test_matrix_results.MatrixCollectionTests.collect

    def test_a_suite_with_cells_on_other_storage_has_a_record_version_of_its_own(self):
        for suite, cells in common.MATRIX_SUITES.items():
            with self.subTest(suite=suite):
                # a suite with reference cells has the version after these (test_reference_cells.py)
                expected = ("rcp-production-public-report-v7" if suite in common.MATRIX_REFERENCE_SUITES else
                            V6 if suite in (*ADDED, *common.MATRIX_WEEKLY_SUITES) else
                            V5 if "+tools" in suite else V4)
                self.assertEqual(projection.matrix_schema(scenario for _, scenario in cells), expected)
        self.assertEqual(projection.MATRIX_SCHEMAS[:3], (V4, V5, V6))

    def test_the_published_variants_are_the_ones_the_runner_runs(self):
        self.assertEqual(projection.PUBLIC_CONTRACTS[V6]["tool_variants"],
                         {scenario: cell["variant"]["id"] for scenario, cell in matrix.TOOLS.items()})
        # the version before it is frozen with the tools it had
        self.assertEqual(sorted(projection.PUBLIC_CONTRACTS[V5]["tool_variants"]), sorted((*LOCAL[1:], *REMOTE[1:])))
        self.assertEqual({key: value for key, value in projection.PUBLIC_CONTRACTS[V6].items() if key != "tool_variants"},
                         {key: value for key, value in projection.PUBLIC_CONTRACTS[V5].items() if key != "tool_variants"})

    def test_a_run_with_cells_everywhere_publishes_each_cell_under_what_it_ran(self):
        result, report, public = self.collect(COMMISSION, matrix_record(COMMISSION))
        self.assertEqual((result["state"], result["retired"], report["benchmark_valid"]), ("success", True, True),
                         public["diagnostics"])
        self.assertEqual(public["schema"], V6)
        self.assertEqual([(cell["case_id"], cell["scenario"], cell["outcome"]) for cell in public["cells"]],
                         [(case, scenario, "complete") for case, scenario in common.MATRIX_SUITES[COMMISSION]])
        self.assertEqual(len(public["measurements"]), 24)
        ran = {(item["case_id"], item["scenario"]): item["variant_id"] for item in public["measurements"]}
        self.assertEqual([ran["commission-10k", scenario] for scenario in NFS],
                         ["rcp-default", "rcmp-size", "rlink-default", "rrm-default"])
        self.assertEqual([ran["commission-10k-memory", scenario] for scenario in MEMORY],
                         ["rcp-default", "rcmp-size", "rlink-default", "rrm-default"])
        # each cell is a line of its own: the same file set through a mount is not the local copy
        self.assertEqual(len({item["series_id"] for item in public["measurements"]}), 24)
        self.assertEqual(public["note"], "Each measurement is the median of 3 copies or tool runs.")
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)), "24 of 24 measurements completed")

    def test_the_cells_of_the_arrays_stay_on_the_lines_they_had(self):
        # a run that also measures on other storage continues every line of the suites before it
        everywhere = self.collect(COMMISSION, matrix_record(COMMISSION))[2]
        arrays = self.collect("matrix-commission-local+tools+remote+tools",
                              matrix_record("matrix-commission-local+tools+remote+tools"))[2]
        self.assertEqual(arrays["schema"], V5)
        lines = {(item["case_id"], item["scenario"]): item["series_id"] for item in everywhere["measurements"]}
        self.assertEqual(len(arrays["measurements"]), 16)
        for item in arrays["measurements"]:
            self.assertEqual(lines[item["case_id"], item["scenario"]], item["series_id"])

    def test_cells_whose_storage_was_missing_are_published_as_failed_with_the_reason(self):
        cells = common.MATRIX_SUITES[COMMISSION]
        missing = {index: dict(status="failed", stage="storage" if scenario == "nfs" else "copy", message=(
            MISSING if scenario == "nfs" else "the nfs copy that this cell follows did not complete: " + MISSING))
            for index, (_, scenario) in enumerate(cells) if scenario in NFS}
        record = matrix_record(COMMISSION, missing, storages=dict(
            nfs=dict(unavailable=MISSING, storage_ids=dict(other_storage.STORAGE_IDS["nfs"])),
            memory=test_matrix_results.STORAGES["memory"]))
        result, report, public = self.collect(COMMISSION, record)
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        published = {(cell["case_id"], cell["scenario"]): cell for cell in public["cells"]}
        self.assertEqual({key: published["commission-10k", "nfs"][key] for key in ("outcome", "stage", "message")},
                         dict(outcome="failed", stage="storage", message=MISSING))
        self.assertEqual(published["commission-10k", "nfs-rrm"]["stage"], "copy")
        self.assertEqual(len(public["measurements"]), 20)
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)), "20 of 24 measurements completed")
        # the cells in memory and on the arrays are measured and published as in any other run
        self.assertEqual(published["commission-10k-memory", "memory"]["outcome"], "complete")
        self.assertEqual(published["commission-10k", "local"]["outcome"], "complete")
        # the run page says in words where the cell failed, as for every other stage
        page = export.render(projection.bind(public, "a" * 64))
        self.assertIn("<td class='reason'>Preparing the storage: " + MISSING + "</td>", page)
        self.assertIn("<td class='reason'>During copy: the nfs copy that this cell follows did not complete: " +
                      MISSING + "</td>", page)

    def test_a_run_of_every_new_suite_is_collected_and_published(self):
        # a suite of one storage alone too: a run in memory keeps no file set on the arrays
        for suite in ADDED:
            options = dict(remaining=42700, runtime_seconds=43200, capacity_seconds=7200) if suite == WEEKLY else {}
            with self.subTest(suite=suite):
                result, report, public = self.collect(suite, matrix_record(suite), **options)
                self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
                self.assertEqual(len(public["measurements"]), len(common.MATRIX_SUITES[suite]))
                self.assertEqual(public["schema"], V6)

    def test_every_cell_of_the_largest_suite_fits_one_record(self):
        result, report, public = self.collect(WEEKLY, matrix_record(WEEKLY), remaining=42700, runtime_seconds=43200,
                                              capacity_seconds=7200)
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        self.assertEqual((len(public["cells"]), len(public["measurements"])), (40, 40))
        self.assertLessEqual(len(common.MATRIX_SUITES[WEEKLY]), projection.MAX_CELLS)


D5 = "rcp-public-resource-diagnostics-v5"
# what a worker reports of the storage it prepared (other_storage.py)
PREPARED = dict(
    nfs=dict(layout="nfs-loopback-v1", version="4.2", threads=256, delay_microseconds=2500,
             round_trip_seconds=0.005123, exports=dict(source="/mnt/rcp-benchmark/source",
                                                       destination="/mnt/rcp-benchmark/destination"),
             mounts=dict(source="/mnt/rcp-benchmark/nfs-source", destination="/mnt/rcp-benchmark/nfs-destination")),
    memory=dict(layout="tmpfs-pair-v1", size_bytes=64 * 1024 ** 3, inodes=24_000_000,
                mounts=dict(source="/mnt/rcp-benchmark/memory-source",
                            destination="/mnt/rcp-benchmark/memory-destination")))
PUBLISHED = dict(nfs=dict(status="prepared", version="4.2", threads=256, delay_microseconds=2500,
                          round_trip_microseconds=5123),
                 memory=dict(status="prepared", size_bytes=64 * 1024 ** 3, inodes=24_000_000))


def companion(suite, other=PREPARED, **changes):
    """A run's public record and its resource companion, as derived from what this worker reported."""
    record, _ = matrix_example(suite, **changes)
    raw = matrix_record(suite, changes.get("outcomes"), environment=test_matrix_diagnostics.ENVIRONMENT)
    raw["timestamp"] = record["timestamp"]
    worker_report = dict(test_matrix_diagnostics.WORKER)
    if other is not None:
        worker_report["other_storage"] = other
    return record, diagnostics.derive(record, b"archive", raw, worker_report)


class StorageDiagnosticsTests(unittest.TestCase):
    """The resource companion of a run with cells on other storage."""

    def test_a_copy_on_other_storage_is_one_process_and_its_tools_run_under_their_own_names(self):
        record, extra = companion(COMMISSION)
        self.assertEqual((extra["schema"], extra["status"], extra["reason"]), (D5, "complete", None))
        self.assertEqual(len(extra["trials"]), 72)
        by_scenario = {}
        for trial in extra["trials"]:
            by_scenario.setdefault(trial["scenario"], []).append(trial)
        expected = {copies: {"rcp-default"} for copies in ("local", "remote", "nfs", "memory")}
        expected.update({copies + "-" + tool: {variant} for copies in ("local", "remote", "nfs", "memory")
                         for tool, variant in (("rcmp", "rcmp-size"), ("rlink", "rlink-default"), ("rrm", "rrm-default"))})
        self.assertEqual({scenario: {trial["variant_id"] for trial in trials} for scenario, trials in by_scenario.items()},
                         expected)
        for scenario, trials in by_scenario.items():
            for trial in trials:
                # only a remote copy starts daemons: a copy through a mount is one rcp process
                self.assertEqual(trial["roles"] is not None, scenario == "remote", scenario)
        self.assertEqual(extra["process_scope"], diagnostics.STORAGE_PROCESS_SCOPE)
        self.assertIn("A local copy, a copy between two mounts of a network filesystem and a copy between two roots "
                      "in memory are each one rcp process", extra["process_scope"])
        diagnostics.validate(extra, record)

    def test_a_suite_of_one_storage_alone_is_not_taken_for_remote_copies(self):
        for suite in ("matrix-commission-nfs+tools", "matrix-commission-memory+tools"):
            record, extra = companion(suite)
            with self.subTest(suite=suite):
                self.assertEqual((extra["schema"], extra["status"]), (D5, "complete"))
                self.assertEqual({trial["roles"] for trial in extra["trials"]}, {None})
                self.assertEqual(len(extra["trials"]), 12)

    def test_what_the_storage_was_is_published_with_the_run(self):
        record, extra = companion(COMMISSION)
        self.assertEqual(extra["other_storage"], PUBLISHED)
        # a suite names only the storage it measures on
        self.assertEqual(companion("matrix-commission-nfs+tools")[1]["other_storage"], dict(nfs=PUBLISHED["nfs"]))
        # storage that could not be prepared, and a worker that reported nothing of it
        failed = dict(PREPARED, nfs=dict(error=dict(type="CalledProcessError", command="modprobe", returncode=1)))
        self.assertEqual(companion(COMMISSION, failed)[1]["other_storage"],
                         dict(nfs=dict(status="unavailable"), memory=PUBLISHED["memory"]))
        self.assertEqual(companion(COMMISSION, None)[1]["other_storage"],
                         dict(nfs=dict(status="not_recorded"), memory=dict(status="not_recorded")))
        # nothing but the known figures leaves the worker's report
        for broken in (dict(PREPARED["nfs"], version="4.2; rm -rf"), dict(PREPARED["nfs"], threads="many"),
                       dict(PREPARED["nfs"], round_trip_seconds=-1), dict(PREPARED["nfs"], round_trip_seconds=True),
                       dict(PREPARED["nfs"], delay_microseconds=10 ** 12), "prepared", None):
            with self.subTest(broken=broken):
                self.assertEqual(companion(COMMISSION, dict(PREPARED, nfs=broken))[1]["other_storage"]["nfs"],
                                 dict(status="not_recorded"))

    def test_the_companion_is_checked_against_its_record(self):
        record, extra = companion(COMMISSION)
        tool = next(index for index, trial in enumerate(extra["trials"]) if trial["scenario"] == "nfs-rrm")
        copy = next(index for index, trial in enumerate(extra["trials"]) if trial["scenario"] == "memory")
        for change in (lambda value: value["trials"][tool].update(variant_id="rcp-default"),
                       lambda value: value["trials"][copy].update(variant_id="rrm-default"),
                       lambda value: value["trials"][copy].update(roles=dict(source=None, destination=None)),
                       lambda value: value.update(schema="rcp-public-resource-diagnostics-v4",
                                                  process_scope=diagnostics.TOOLS_PROCESS_SCOPE),
                       lambda value: value.pop("other_storage"),
                       lambda value: value["other_storage"].update(tape=dict(status="prepared")),
                       lambda value: value["other_storage"].pop("memory"),
                       lambda value: value["other_storage"]["nfs"].update(version="5"),
                       lambda value: value["other_storage"]["nfs"].update(round_trip_microseconds=5.5),
                       lambda value: value["other_storage"]["nfs"].update(mounts="/mnt"),
                       lambda value: value["other_storage"]["memory"].update(status="unavailable")):
            broken = json.loads(json.dumps(extra))
            change(broken)
            with self.subTest(change=change), self.assertRaises(ValueError):
                diagnostics.validate(broken, record)

    def test_a_run_without_usable_diagnostics_still_has_a_companion(self):
        # no archive, an archive that is not the runner's, a result over a limit: the pages are
        # written all the same, with a companion that says nothing was recorded
        record, _ = companion(COMMISSION)
        for reason in ("not_recorded", "invalid_diagnostics", "not_matched"):
            with self.subTest(reason=reason):
                extra = diagnostics.unavailable(record, reason)
                self.assertEqual((extra["schema"], extra["status"], extra["trials"]), (D5, "unavailable", []))
                self.assertEqual(extra["other_storage"],
                                 dict(nfs=dict(status="not_recorded"), memory=dict(status="not_recorded")))
                diagnostics.validate(extra, record)
                self.assertIn("were not recorded for this run", diagnostics.render(extra, record))
        alone, _ = companion("matrix-commission-memory+tools")
        self.assertEqual(diagnostics.unavailable(alone)["other_storage"], dict(memory=dict(status="not_recorded")))
        # a result the derivation refuses takes that way too
        raw = matrix_record(COMMISSION, environment=test_matrix_diagnostics.ENVIRONMENT)
        raw["file_sets"][0]["case"]["id"] = "some-other-set"
        refused = diagnostics.derive(record, b"archive", raw, test_matrix_diagnostics.WORKER)
        self.assertEqual((refused["status"], refused["reason"]), ("unavailable", "invalid_diagnostics"))
        self.assertIn("other_storage", refused)
        # the companions of earlier records are what they were
        earlier, _ = companion("matrix-commission-local+tools")
        self.assertNotIn("other_storage", diagnostics.unavailable(earlier))

    def test_a_run_in_memory_alone_reports_no_capacity_of_the_arrays_and_has_its_diagnostics(self):
        record, _ = companion("matrix-commission-memory+tools")
        raw = matrix_record("matrix-commission-memory+tools", environment=test_matrix_diagnostics.ENVIRONMENT)
        raw["timestamp"] = record["timestamp"]
        reported = test_matrix_diagnostics.WORKER
        # what the worker reports when no file set is kept on the arrays, and reports a reader must survive
        for capacity in ({}, None, "none", 7):
            with self.subTest(capacity=capacity):
                worker_report = dict(reported, storage=dict(reported["storage"], capacity=capacity),
                                     other_storage=dict(memory=PREPARED["memory"]))
                extra = diagnostics.derive(record, b"archive", raw, worker_report)
                self.assertEqual((extra["status"], len(extra["trials"])), ("complete", 12))
                self.assertEqual([extra["setup"][side]["block_bytes"] for side in ("source", "destination")],
                                 [None, None])
                # the arrays themselves are described as in any other run
                self.assertEqual(extra["setup"]["source"]["layout"], "two-disjoint-raid0")
        # and a report whose other parts are not what they should be costs figures, not the companion
        for broken in (dict(reported, storage=None), dict(reported, storage=dict(reported["storage"], arrays=None)),
                       dict(reported, storage=dict(reported["storage"], arrays=dict(source=3)))):
            extra = diagnostics.derive(record, b"archive", raw, broken)
            self.assertEqual(extra["status"], "complete")

    def test_earlier_companions_keep_their_version_and_have_no_such_part(self):
        for suite, schema in (("matrix-commission-local", "rcp-public-resource-diagnostics-v2"),
                              ("matrix-commission-local+remote", "rcp-public-resource-diagnostics-v3"),
                              ("matrix-commission-local+tools+remote+tools", "rcp-public-resource-diagnostics-v4")):
            record, extra = companion(suite)
            with self.subTest(suite=suite):
                self.assertEqual(extra["schema"], schema)
                self.assertNotIn("other_storage", extra)
                diagnostics.validate(extra, record)

    def test_every_file_set_of_the_largest_suite_has_its_progress(self):
        record, extra = companion(WEEKLY)
        self.assertEqual((extra["status"], len(extra["trials"]), len(extra["cases"])), ("complete", 120, 6))
        self.assertEqual([case["case_id"] for case in extra["cases"]], [*FILE_SETS, NFS_SET, MEMORY_SET])
        self.assertLessEqual(len(extra["trials"]), diagnostics.MAX_TRIALS)

    def test_the_sampled_detail_of_the_largest_suite_fits_the_journal_and_the_public_document(self):
        descriptor = diagnostics.TELEMETRY_DESCRIPTORS[-1]
        self.assertEqual(descriptor, telemetry.descriptor(telemetry.MATRIX_LIMITS))
        raw, total = sampled_matrix(WEEKLY, descriptor)
        # six file sets; a cache step and a command for a copy, none dropped in memory; a tool run
        # and what rlink leaves to clear; on a network filesystem a cache step before every tool too
        self.assertEqual(total, 19 + 57 + 96 + 21 + 12)
        self.assertLess(total, descriptor["max_phases"] - 64)
        # the journals before had room for fewer phases than this suite marks
        self.assertGreater(total, diagnostics.TELEMETRY_DESCRIPTORS[1]["max_phases"])
        record, _ = matrix_example(WEEKLY)
        extra = diagnostics.derive(record, b"archive", raw, dict(test_matrix_diagnostics.WORKER, other_storage=PREPARED))
        # nothing had to be left out to stay within the public document's size
        self.assertEqual((extra["status"], extra["reason"]), ("complete", None))
        self.assertEqual(sum(len(trial["system"]["phases"]) for trial in extra["trials"]) +
                         len(extra["system"]["phases"]), total)
        size = len(json.dumps(extra, separators=(",", ":")))
        self.assertLess(size, diagnostics.MAX_DOCUMENT * 3 // 4, size)
        # and well inside what one record may bring to an export
        self.assertLessEqual(diagnostics.MAX_DOCUMENT, collection.MAX_DOCUMENT)
        html = diagnostics.render(extra, record)
        self.assertIn("Sampled phases: tiny-1k-700x1024 / nfs-rrm / run 1", html)
        self.assertIn("Sampled phases: tiny-1k-10240k-memory / memory / copy 3", html)
        self.assertLess(len(html.encode()), projection.MAX_PUBLIC_HTML // 4)

    def test_the_run_page_names_the_process_of_each_run_and_what_the_storage_was(self):
        record, extra = companion(COMMISSION)
        html = diagnostics.render(extra, record)
        self.assertIn("<th>File set</th><th>Scenario</th><th>Copy or run</th><th>Process</th>", html)
        for case, scenario, process in (("commission-10k", "nfs", "rcp"), ("commission-10k", "nfs-rcmp", "rcmp"),
                                        ("commission-10k", "nfs-rlink", "rlink"), ("commission-10k", "nfs-rrm", "rrm"),
                                        ("commission-10k-memory", "memory", "rcp"),
                                        ("commission-10k-memory", "memory-rrm", "rrm"),
                                        ("commission-10k", "remote", "source rcpd")):
            self.assertIn("<td><code>" + case + "</code></td><td><code>" + scenario + "</code></td><td>1</td><td>" +
                          process + "</td>", html)
        self.assertNotIn("<td><code>nfs</code></td><td>1</td><td>source rcpd</td>", html)
        self.assertIn("<h3 id='other-storage'>Other storage of this run</h3>", html)
        self.assertIn("<td>NFS 4.2</td><td>5.123 ms</td><td>2.5 ms on every packet of the loopback</td>"
                      "<td>256</td>", html)
        self.assertIn("<td>two filesystems in memory</td><td>64 GiB each</td><td>24,000,000 each</td>", html)
        # a storage that was not there says so, and nothing is made up for it
        failed = dict(PREPARED, nfs=dict(error=dict(type="OSError")))
        record, extra = companion(COMMISSION, failed)
        self.assertIn("The mounts of the network filesystem could not be prepared for this run", diagnostics.render(extra, record))
        self.assertNotIn("other-storage", diagnostics.render(*companion("matrix-commission-local+tools")[::-1]))


class StoragePageTests(unittest.TestCase):
    """The pages take a run with cells on other storage; their trends are in test_storage_pages.py."""

    def test_every_trend_page_and_the_first_page_render_for_runs_of_the_largest_suite(self):
        records, companions = split(weeks(2, suite=WEEKLY))
        for trend in history.present(records):
            with self.subTest(trend=trend):
                cases, outside = history.model(records, companions, trend)
                self.assertEqual(({len(points) for points in cases.values()}, outside), ({2}, 0))
                self.assertLess(len(history.render(records, companions, trend).encode()), projection.MAX_PUBLIC_HTML // 8)
        self.assertLess(len(history.overview(records, companions).encode()), projection.MAX_PUBLIC_HTML // 8)

    def test_the_run_page_lists_every_cell_and_its_diagnostics(self):
        record, extra = companion(COMMISSION)
        page = export.render(record, diagnostics=extra)
        for case, scenario in common.MATRIX_SUITES[COMMISSION]:
            self.assertIn("<code>" + scenario + "</code>", page)
        self.assertIn("24 of 24 measurements completed", page)
        self.assertIn("<h3 id='other-storage'>Other storage of this run</h3>", page)
        self.assertIn("NFS 4.2", page)


class StorageImageTests(unittest.TestCase):
    """An image that cannot serve these cells fails its build, and the check leaves nothing behind."""

    def test_the_probe_runs_under_the_sandbox_of_the_measurement_unit(self):
        data = Path("/var/lib/rcp-storage-check")
        argv = storage_check.command(data)
        self.assertEqual(argv[:2], ["/usr/bin/systemd-run", "--unit=rcp-storage-check"])
        self.assertTrue({"--wait", "--collect"} <= set(argv))
        expected = properties(worker.command(dict(suite="matrix-commission-nfs+tools", baseline="a" * 40), 180))
        # the check's directories stand in for the data mounts, and the build log must say why it failed
        expected.update(ReadWritePaths=str(data), StandardOutput="journal+console", StandardError="journal+console")
        self.assertEqual(properties(argv), expected)
        self.assertEqual((expected["User"], expected["PrivateNetwork"], expected["RestrictAddressFamilies"]),
                         ("rcp-benchmark", "yes", "AF_UNIX"))
        self.assertIn("--setenv=PYTHONPATH=/opt/rcp-benchmark", argv)
        self.assertEqual(argv[-8:], ["/usr/bin/python3", "-B", "-c", storage_check.PROBE,
                                     f"{data}/nfs-source:{data}/source", f"{data}/nfs-destination:{data}/destination",
                                     f"{data}/memory-source:", f"{data}/memory-destination:"])

    def probe(self, *roots):
        return subprocess.run([sys.executable, "-B", "-c", storage_check.PROBE, *roots], capture_output=True,
                              text=True, timeout=60)

    def test_the_probe_writes_through_a_mount_and_finds_the_file_where_the_tree_is_kept(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("source", "elsewhere", "memory"):
                (root / name).mkdir()
            # a mount shows the same tree under another path, as this link does
            (root / "mount").symlink_to(root / "source")
            done = self.probe(f"{root / 'mount'}:{root / 'source'}", f"{root / 'memory'}:")
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertEqual([list((root / name).iterdir()) for name in ("source", "memory")], [[], []])
            # a mount that shows some other tree is not the export
            self.assertNotEqual(self.probe(f"{root / 'mount'}:{root / 'elsewhere'}").returncode, 0)
            self.assertEqual(sorted(path.name for path in (root / "source").iterdir()), ["probe", "probe.link"])
            # and a root that cannot be written to fails it
            self.assertNotEqual(self.probe(f"{root / 'missing'}:").returncode, 0)

    def test_everything_the_preparations_made_is_taken_down_whatever_fails_on_the_way(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data, state = root / "data", root / "state"
            kernel = other_storage.Kernel(pipes=root / "run/rpc_pipefs", server=root / "proc/fs/nfsd")
            commands = []

            def execute(argv, budget, **kwargs):
                commands.append(list(argv))
                if argv[0] in refused:
                    raise subprocess.CalledProcessError(1, argv)

            for refused in ((), ("umount", "tc")):
                commands.clear()
                (data / "source").mkdir(parents=True)
                state.mkdir()
                left = storage_check.teardown(data, state, object(), execute, kernel=kernel)
                self.assertEqual(commands, [
                    *(["umount", str(data / name)] for name in ("nfs-source", "nfs-destination", "memory-source",
                                                                 "memory-destination")),
                    ["rpc.nfsd", "0"], ["exportfs", "-ua"],
                    ["systemctl", "stop", "rcp-nfs-mountd.service"], ["systemctl", "stop", "rcp-nfsdcld.service"],
                    ["tc", "qdisc", "del", "dev", "lo", "root"],
                    ["umount", str(kernel.pipes)], ["umount", str(kernel.server)],
                    ["sysctl", "--quiet", "--write", "fs.leases-enable=1"]])
                self.assertEqual(left, [f"umount {data / name}" for name in (
                    "nfs-source", "nfs-destination", "memory-source", "memory-destination")] + [
                    "tc qdisc", f"umount {kernel.pipes}", f"umount {kernel.server}"] if refused else [])
                self.assertFalse(data.exists() or state.exists())
            # directories that cannot be removed are named too: a mount is still there
            self.assertEqual(storage_check.teardown(data, state, object(), execute, kernel=kernel)[-2:],
                             [str(data), str(state)])

    def run_check(self, *, nfs=None, memory=None, probe=0, left=()):
        """Run the check with stand-ins for the preparations, the probe and the teardown."""
        with tempfile.TemporaryDirectory() as temporary, contextlib.redirect_stdout(io.StringIO()) as printed, \
                mock.patch.object(storage_check, "DATA", Path(temporary) / "data"), \
                mock.patch.object(storage_check, "STATE", Path(temporary) / "state"), \
                mock.patch.object(storage_check.os, "geteuid", return_value=0), \
                mock.patch.object(storage_check.os, "chown") as chown, \
                mock.patch.object(storage_check.pwd, "getpwnam", return_value=mock.Mock(pw_uid=123, pw_gid=456)), \
                mock.patch.object(other_storage, "prepare_nfs", side_effect=nfs, return_value=dict(
                    version="4.2", round_trip_seconds=0.00512)) as prepare_nfs, \
                mock.patch.object(other_storage, "prepare_memory", side_effect=memory) as prepare_memory, \
                mock.patch.object(storage_check.subprocess, "run", side_effect=probe if isinstance(
                    probe, Exception) else None, return_value=mock.Mock(returncode=probe)) as run, \
                mock.patch.object(storage_check, "teardown", return_value=list(left)) as teardown:
            try:
                storage_check.main()
                failure = None
            except SystemExit as error:
                failure = str(error)
            return failure, printed.getvalue(), dict(nfs=prepare_nfs, memory=prepare_memory, run=run,
                                                     teardown=teardown, chown=chown, data=storage_check.DATA)

    def test_the_check_prepares_both_probes_and_takes_them_down(self):
        failure, printed, calls = self.run_check()
        self.assertIsNone(failure)
        self.assertIn("NFS 4.2 mounts with a round trip of 5.12 ms", printed)
        data = calls["data"]
        # two directories of the benchmark account stand in for the arrays
        self.assertEqual([call.args for call in calls["chown"].call_args_list],
                         [(data / "source", 123, 456), (data / "destination", 123, 456)])
        self.assertEqual(calls["nfs"].call_args.args[:2], (data, data.parent / "state"))
        self.assertIs(calls["nfs"].call_args.args[3], worker.execute)
        self.assertEqual(calls["memory"].call_args.args[2:], (123, 456, worker.execute))
        self.assertEqual(calls["run"].call_args.args[0], storage_check.command(data))
        self.assertEqual(calls["teardown"].call_count, 1)

    def test_a_failure_fails_the_build_and_everything_is_still_taken_down(self):
        refused = subprocess.CalledProcessError(1, ["modprobe", "--all", "sch_netem"])
        for changes, expected in (
                (dict(nfs=refused), "cannot serve benchmark cells on other storage: CalledProcessError: .*modprobe"),
                (dict(nfs=ValueError("an NFS round trip takes 0.21 ms, not the intended one")), "round trip takes 0.21 ms"),
                (dict(memory=OSError("mount point busy")), "cannot serve benchmark cells on other storage: OSError"),
                (dict(probe=1), "the probe under the measurement sandbox exited with status 1"),
                (dict(probe=subprocess.TimeoutExpired(["/usr/bin/systemd-run"], 240)), "systemd-run timed out"),
                (dict(left=["umount /var/lib/rcp-storage-check/nfs-source"]),
                 "could not take down what it prepared: umount /var/lib/rcp-storage-check/nfs-source")):
            with self.subTest(changes=changes):
                failure, printed, calls = self.run_check(**changes)
                self.assertRegex(failure or "", expected)
                self.assertEqual(printed, "")
                self.assertEqual(calls["teardown"].call_count, 1)
                # nothing is probed on storage that could not be prepared
                self.assertEqual(calls["run"].call_count, 0 if "nfs" in changes or "memory" in changes else 1)
        with mock.patch.object(storage_check.os, "geteuid", return_value=1000), \
                self.assertRaisesRegex(SystemExit, "requires root"):
            storage_check.main()

    def test_the_image_installs_the_server_and_runs_nothing_of_it(self):
        installer = (ROOT / "infra/aws_benchmark/install-worker.sh").read_text()
        packages = next(line for line in installer.splitlines() if "apt-get install" in line)
        for package in ("nfs-kernel-server", "iproute2", "kmod"):
            self.assertIn(f" {package} ", packages + " ")
        copied = installer.split("cp infra/aws_benchmark/{", 1)[1].split("}", 1)[0].split(",")
        self.assertLessEqual({"other_storage", "storage_check"}, set(copied))
        # the supervisor starts its own server for one run and refuses a machine where one runs
        # already: every service and socket of the packages is masked, and before the packages
        # arrive, since their installation would start the server
        units = installer.split("nfs_units=(", 1)[1].split(")", 1)[0].split()
        # what the packages enable, and what those pull in
        for unit in ("nfs-server.service", "rpcbind.service", "rpcbind.socket", "nfs-client.target",
                     "nfs-blkmap.service", "fsidd.service", "nfs-mountd.service", "nfsdcld.service",
                     "rpc-statd.service", "nfs-idmapd.service", "auth-rpcgss-module.service"):
            self.assertIn(unit, units)
        # the supervisor mounts the two kernel filesystems itself, where their mount units would:
        # those units stay as they are, and so does a target of systemd itself
        for unit in ("proc-fs-nfsd.mount", "run-rpc_pipefs.mount", "rpc_pipefs.target", "rpcbind.target"):
            self.assertNotIn(unit, units)
        self.assertEqual({str(other_storage.Kernel().server), str(other_storage.Kernel().pipes)},
                         {"/proc/fs/nfsd", "/run/rpc_pipefs"})
        mask = 'systemctl mask "${nfs_units[@]}"\n'
        self.assertEqual(installer.count(mask), 1)
        self.assertLess(installer.index(mask), installer.index("apt-get install"))
        # and none of them runs once the packages are in
        check = installer.split("apt-get install", 1)[1].split("useradd", 1)[0]
        self.assertIn('for unit in "${nfs_units[@]}"; do\n  test "$(systemctl is-active "$unit" || true)" != active\n'
                      "done\n", check)
        # the check runs on the installed tree, after the remote one and before the image is sealed
        after = installer.split("progress verify-remote-cells", 1)[1]
        self.assertIn("progress verify-storage-cells\npython3 -B -m infra.aws_benchmark.storage_check\n", after)
        self.assertLess(after.index("verify-storage-cells"), after.index("progress output-contract"))


if __name__ == "__main__":
    unittest.main()
