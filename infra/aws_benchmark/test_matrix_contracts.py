"""Matrix suites are an exact catalog: a name fixes its cells, copy count and time budget."""

import json
from pathlib import Path
import unittest
from unittest import mock

from benchmarks import cache, matrix, run
from . import common, worker

ROOT = Path(__file__).resolve().parents[2]

FILE_SETS = ("bulk-1g-1tb", "bulk-100m-1024gb", "bulk-1m-1024gb", "tiny-1k-10240k")


class MatrixContractTests(unittest.TestCase):
    def test_the_catalog_covers_every_file_set_alone_all_together_and_commissioning(self):
        # the suites of the arrays; those with cells on other storage follow them (test_storage_cells.py)
        self.assertEqual(sorted(common.DURABLE_SUITES[5:41]), sorted(
            f"matrix-{selection}-{scenarios}" for selection in ("all", "1g", "100m", "1m", "1k", "commission")
            for scenarios in ("local", "remote", "local+remote", "local+tools", "local+tools+remote",
                              "local+tools+remote+tools")))
        self.assertEqual(common.MATRIX_SUITES["matrix-all-local"], tuple((case, "local") for case in FILE_SETS))
        self.assertEqual(common.MATRIX_SUITES["matrix-all-remote"], tuple((case, "remote") for case in FILE_SETS))
        # both scenarios of a file set run next to each other, on the one fixture generated for it
        self.assertEqual(common.MATRIX_SUITES["matrix-all-local+remote"],
                         tuple((case, scenario) for case in FILE_SETS for scenario in ("local", "remote")))
        self.assertEqual(common.MATRIX_SUITES["matrix-1k-remote"], (("tiny-1k-10240k", "remote"),))
        self.assertEqual(common.MATRIX_SUITES["matrix-commission-local+remote"], (
            ("commission-64m", "local"), ("commission-64m", "remote"),
            ("commission-10k", "local"), ("commission-10k", "remote")))
        self.assertEqual(common.SUITES["matrix-all-local+remote"], FILE_SETS)
        # the suites of the first matrix release keep their place and their plans
        self.assertEqual(common.DURABLE_SUITES[5:11], (
            "matrix-all-local", "matrix-1g-local", "matrix-100m-local", "matrix-1m-local", "matrix-1k-local",
            "matrix-commission-local"))
        self.assertEqual(len(common.DURABLE_SUITES), 47)
        self.assertEqual(common.MATRIX_SUITES["matrix-1m-local"], (("bulk-1m-1024gb", "local"),))
        self.assertEqual(common.MATRIX_SUITES["matrix-commission-local"],
                         (("commission-64m", "local"), ("commission-10k", "local")))
        self.assertEqual(common.SUITES["matrix-all-local"], FILE_SETS)
        for suite in ("bulk-1g-metadata", "all-filesets-metadata", "matrix-all", "unknown"):
            with self.subTest(suite=suite), self.assertRaisesRegex(ValueError, "not a benchmark matrix"):
                common.matrix_contract(suite)

    def test_every_scenario_is_one_the_runner_knows(self):
        self.assertEqual(common.MATRIX_POLICY, matrix.POLICY)
        for suite in common.MATRIX_SUITES:
            self.assertLessEqual({scenario for _, scenario in common.MATRIX_SUITES[suite]}, set(matrix.SCENARIOS))

    def test_the_contract_is_one_variant_three_copies_and_a_hang_only_limit(self):
        contract = common.matrix_contract("matrix-all-local")
        self.assertEqual(contract, dict(cells=common.MATRIX_SUITES["matrix-all-local"], repetitions=3,
            timeout=3600, purpose="performance", cache_policy="linux-drop-caches",
            runner_label="aws-c5d24-ext4-raid0-matrix-v1"))
        self.assertEqual(common.verification_contract("matrix-all-local"), {"policy": "exit-status-summary-v1"})
        self.assertEqual(common.matrix_contract("matrix-commission-local")["purpose"], "commissioning")
        # matrix suites exist only on the direct path and never read a copied tree
        for suites in (common.ACTIVE_SUITES, common.LARGE_SUITES, common.METADATA_SUITES):
            self.assertFalse(set(suites) & set(common.MATRIX_SUITES))
        self.assertEqual(common.verification_contracts("matrix-1g-local"), ({"policy": "exit-status-summary-v1"},))

    def test_time_budgets_follow_the_size_of_the_selection(self):
        full = common.durable_plan_contract(common.durable_plan_for_suite("matrix-all-local"))
        self.assertEqual((full["max_runtime"], full["max_provisioning"], full["max_attempts"]), (28800, 7200, 120))
        self.assertEqual((full["runtime_profile"], full["reservation_microusd"]), ("all-filesets-8h-v1", 50_000_000))
        self.assertEqual(full["plan"]["minimum_worker_seconds"], 3600)
        everything = {"matrix-all-local", "matrix-all-remote", "matrix-all-local+remote",
                      "matrix-all-local+tools", "matrix-all-local+tools+remote",
                      "matrix-all-local+tools+remote+tools"}
        for suite in sorted(everything):
            contract = common.durable_plan_contract(common.durable_plan_for_suite(suite))
            # a suite that times the other tools sends more binaries and has more time: twelve hours
            # and the reservation that goes with them (test_tool_allocation.py)
            skipped = ("suite", "plan", "binary_names", "binaries")
            longer = dict(max_runtime=43200, runtime_profile="all-filesets-12h-v1", reservation_microusd=65_000_000) \
                if "tools" in suite else {}
            self.assertEqual({key: contract[key] for key in full if key not in skipped},
                             {key: full[key] for key in full if key not in skipped} | longer)
            version = suite + ("-plan-v2" if "tools" in suite else "-plan-v1")
            self.assertEqual(contract["plan"], dict(full["plan"], version=version))
        for suite in sorted(set(common.DURABLE_SUITES[5:41]) - everything):
            with self.subTest(suite=suite):
                contract = common.durable_plan_contract(common.durable_plan_for_suite(suite))
                self.assertEqual((contract["suite"], contract["max_runtime"]), (suite, 10800))
                self.assertNotIn("runtime_profile", contract)
                self.assertNotIn("reservation_microusd", contract)
                # a slow start leaves less time, never a refused allocation: one hour is the floor
                self.assertEqual(common.durable_worker_reserve(common.durable_plan_for_suite(suite)), 3600)
        versions = [common.durable_plan_for_suite(suite)["version"] for suite in common.MATRIX_SUITES]
        self.assertEqual(len(set(versions)), len(versions))

    def test_public_diagnostics_know_the_shape_of_every_matrix_file_set(self):
        from . import production_diagnostics as diagnostics
        manifest = json.loads((ROOT / "benchmarks/matrix-cases.json").read_text())
        shapes = {case["id"]: {key: case[key] for key in ("directory_widths", "files_per_leaf", "file_size_bytes")}
                  for case in manifest["cases"]}
        # the server does not ship the manifest, so its own table must say the same
        self.assertEqual(diagnostics.MATRIX_CASE_SHAPES, shapes)
        self.assertEqual(set(shapes), {case for cells in common.MATRIX_SUITES.values() for case, _ in cells})

    def test_the_image_build_proves_the_runner_imports_as_the_benchmark_user(self):
        installer = (ROOT / "infra/aws_benchmark/install-worker.sh").read_text()
        check = installer.split("runuser -u rcp-benchmark -- /usr/bin/python3 -B -c", 1)[1].split("progress", 1)[0]
        self.assertIn("import benchmarks.matrix", check)
        self.assertIn("cp -r benchmarks /opt/rcp-benchmark/", installer)

    def test_existing_suites_and_plans_are_unchanged(self):
        self.assertEqual(common.durable_plan_for_suite("bulk-1g-metadata"), dict(
            version="bulk-metadata-plan-v1", work_seconds=3104, setup_seconds=100, report_seconds=180,
            safety_factor=3, boot_reserve_seconds=300, shutdown_reserve_seconds=300))
        self.assertEqual(common.durable_plan_for_suite("all-filesets-metadata")["version"],
                         "all-filesets-qualification-budget-v1")
        self.assertEqual(common.measurement_contract("bulk-1g-metadata"), dict(paired_seed=0, repetitions=2,
            purpose="performance", cache_policy="linux-drop-caches", runner_label="aws-c5d24-ext4-raid0-metadata-v1"))
        self.assertEqual(common.DURABLE_SUITES[:5], ("bulk-1g-metadata", "bulk-100m-metadata", "bulk-1m-metadata",
                                                    "tiny-1k-metadata", "all-filesets-metadata"))

    def test_the_worker_runs_the_matrix_runner_with_the_suite_cells_and_no_baseline(self):
        for suite in common.MATRIX_SUITES:
            with self.subTest(suite=suite):
                argv = worker.command(dict(suite=suite, baseline="a" * 40), 25000)
                start = argv.index("benchmarks.matrix")
                args = matrix._arguments(argv[start + 1:])
                contract = common.matrix_contract(suite)
                self.assertEqual(args.cells, [case + ":" + scenario for case, scenario in contract["cells"]])
                self.assertEqual((args.repetitions, args.timeout, args.cache, args.purpose, args.runner_label),
                                 (3, 3600, "linux-drop-caches", contract["purpose"], contract["runner_label"]))
                self.assertEqual(args.bin_dir, worker.STATE / "candidate")
                # CPU and memory are what rcp reports about itself: nothing measures it from outside
                self.assertNotIn("--resource-time", argv)
                self.assertNotIn("/usr/bin/time", argv)
                self.assertTrue(args.check_capacity)
                self.assertTrue(args.system_telemetry)
                self.assertEqual(args.manifest, worker.ROOT / "benchmarks/matrix-cases.json")
                self.assertEqual((args.source_storage_id, args.destination_storage_id),
                                 ("c5d-raid0-ext4-source-v1", "c5d-raid0-ext4-destination-v1"))
                self.assertEqual(args.output, worker.DATA / "output" / "results")
                # one variant: the reference build is neither passed to the runner nor named to it
                self.assertFalse(any("baseline" in item.lower() for item in argv))
                # the allocation reaches the runner only as its deadline, never as a copy timeout
                deadlines = [item for item in argv if item.startswith("--setenv=" + cache.WORKER_DEADLINE_ENV + "=")]
                self.assertEqual(len(deadlines), 1)
                self.assertIn("--property=LimitFSIZE=1073741824", argv)
                self.assertIn("--property=RuntimeMaxSec=25000", argv)

    def test_legacy_suites_keep_their_paired_command(self):
        argv = worker.command(dict(suite="bulk-1g-metadata", baseline="a" * 40), 600)
        self.assertIn("benchmarks.run", argv)
        self.assertIn("--baseline-bin-dir", argv)
        # the paired harness measures each command with GNU time, as before
        self.assertEqual(argv[argv.index("--resource-time") + 1], "/usr/bin/time")
        self.assertIn("--setenv=RCP_BENCH_BASELINE_COMMIT=" + "a" * 40, argv)
        self.assertEqual(argv[argv.index("--timeout") + 1], "600")

    def test_only_plans_with_a_runtime_profile_extend_the_supervisor(self):
        for suite in ("matrix-1g-local", "matrix-commission-local", "bulk-1g-metadata"):
            window = dict(suite=suite, plan=common.durable_plan_for_suite(suite))
            self.assertIsNone(worker.extend_supervisor_runtime(window, None, None, None, None))
        for suite in ("matrix-all-local", "all-filesets-metadata"):
            window = dict(suite=suite, plan=common.durable_plan_for_suite(suite))
            # the extension is attempted: it gets as far as its first requirement, being root
            with self.subTest(suite=suite), mock.patch.object(worker.os, "geteuid", return_value=1000), \
                    self.assertRaisesRegex(ValueError, "requires root"):
                worker.extend_supervisor_runtime(window, None, None, None, None)

    def test_the_matrix_manifest_defines_every_cell_and_matches_the_large_file_sets(self):
        root = Path(__file__).resolve().parents[2] / "benchmarks"
        manifest = run.load_manifest(root / "matrix-cases.json")
        cases = {case["id"]: case for case in manifest["cases"]}
        large = {case["id"]: case for case in json.loads((root / "large-cases.json").read_text())["cases"]}
        for case_id in FILE_SETS:
            self.assertEqual(cases[case_id], large[case_id])
        for suite in common.MATRIX_SUITES:
            matrix.selection(manifest, [case + ":" + scenario for case, scenario in common.MATRIX_SUITES[suite]])
        self.assertEqual(manifest["variants"], [dict(id="rcp-default", description="rcp default copy", tool="rcp",
                                                     args=["--summary"], processes=1)])
        # commissioning fixtures stay small enough to finish in seconds
        for case_id in ("commission-64m", "commission-10k"):
            counts = matrix.operations.expected_counts(cases[case_id])
            self.assertLessEqual(counts["bytes"], 64_000_000)
            self.assertLessEqual(counts["files"], 10_000)


if __name__ == "__main__":
    unittest.main()
