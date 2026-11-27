"""Reference cells: the suites that copy a file set with cp and rsync beside rcp, and what is collected of them."""

from datetime import timedelta
import hashlib
import json
from pathlib import Path
from types import MappingProxyType
import unittest
from unittest import mock

from benchmarks import matrix, telemetry
from . import common, production_diagnostics as diagnostics, production_export as export
from . import production_projection as projection, test_matrix_diagnostics, test_matrix_results, worker
from .test_matrix_admission import weekly
from .test_matrix_pages import matrix_example
from .test_matrix_results import REFERENCE_VERSIONS, matrix_record
from .test_production_policy import NOW as ADMITTED
from .test_storage_cells import PREPARED, companion, properties
from .test_tool_allocation import approve, by_hand
from .test_tool_cells import sampled_matrix, sampled_phase

ROOT = Path(__file__).resolve().parents[2]
# the one run that measures cp and rsync, and the run that tries its steps on the small fixtures
SUITE, COMMISSION = "matrix-reference-1", "matrix-commission-reference"
WEEKLY = common.MATRIX_WEEKLY
ARRAYS = ("local", "local-cp", "local-rsync", "remote", "remote-rsync")
MOUNTS = ("nfs", "nfs-cp", "nfs-rsync")
REFERENCES = ("local-cp", "local-rsync", "remote-rsync", "nfs-cp", "nfs-rsync")
V4, V5, V6, V7 = (f"rcp-production-public-report-v{number}" for number in (4, 5, 6, 7))
D5, D6 = "rcp-public-resource-diagnostics-v5", "rcp-public-resource-diagnostics-v6"
TWELVE_HOURS = dict(remaining=42700, runtime_seconds=43200, capacity_seconds=7200)
CUT = dict(status="timed_out", stage="copy",
           message="copy 1 did not finish within 6002.0 s, 10 times the median of the local cell")


def plain(value):
    if isinstance(value, (dict, MappingProxyType)):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    return value


def digest(value):
    return hashlib.sha256(json.dumps(plain(value), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def index(suite, case, scenario):
    return common.MATRIX_SUITES[suite].index((case, scenario))


class ReferenceSuiteTests(unittest.TestCase):
    def test_the_reference_cells_are_the_runners(self):
        self.assertEqual(dict(common.MATRIX_REFERENCE_CELLS),
                         {scenario: cell["variant"]["id"] for scenario, cell in matrix.REFERENCES.items()})
        self.assertEqual(list(common.MATRIX_REFERENCE_CELLS), list(matrix.REFERENCES))
        self.assertEqual({tool: beside for tool, (_, beside) in common.MATRIX_REFERENCES.items()},
                         {tool: tuple(cell["beside"]) for tool, cell in matrix.REFERENCE_RUNS.items()})
        self.assertEqual(common.MATRIX_REFERENCE_LIMIT, matrix.REFERENCE_LIMIT)
        # a reference cell keeps its trees where the cell beside it does
        for scenario in common.MATRIX_REFERENCE_CELLS:
            self.assertEqual(common.matrix_storage(scenario), matrix.storage_of(scenario), scenario)
        self.assertFalse(set(common.MATRIX_REFERENCE_CELLS) & common.MATRIX_TOOL_CELLS)

    def test_one_suite_copies_what_the_weekly_suite_copies_with_cp_and_rsync_beside_rcp(self):
        cells = common.MATRIX_SUITES[SUITE]
        self.assertEqual(cells, (
            *(("tiny-1k-2048k", scenario) for scenario in ARRAYS),
            *(("tiny-1k-700x1024", scenario) for scenario in MOUNTS),
            *(("bulk-1m-200gb", scenario) for scenario in ARRAYS),
            *(("bulk-1g-200gb", scenario) for scenario in ARRAYS)))
        # every copy that the weekly suite makes where a disk or a server is, and none in memory
        copies = {cell for cell in cells if cell[1] in matrix.COPIES}
        self.assertEqual(copies, {cell for cell in common.MATRIX_SUITES[WEEKLY] if cell[1] in ("local", "remote", "nfs")})
        # each has its reference cells: cp where it can make the copy, rsync everywhere
        self.assertEqual({cell for cell in cells if cell[1] not in matrix.COPIES},
                         {(case, copies + "-" + tool) for case, copies in copies for tool in ("cp", "rsync")
                          if copies + "-" + tool in matrix.REFERENCES})
        self.assertEqual({scenario for _, scenario in cells} - set(matrix.COPIES), set(REFERENCES))
        self.assertFalse({scenario for _, scenario in cells} & common.MATRIX_TOOL_CELLS)
        self.assertEqual(common.SUITES[SUITE], ("tiny-1k-2048k", "tiny-1k-700x1024", "bulk-1m-200gb", "bulk-1g-200gb"))

    def test_the_commissioning_suite_has_every_kind_of_cell_of_the_run_on_the_small_fixtures(self):
        cells = common.MATRIX_SUITES[COMMISSION]
        self.assertEqual(cells, (*(("commission-64m", scenario) for scenario in ARRAYS),
                                 *(("commission-10k", scenario) for scenario in (*ARRAYS, *MOUNTS))))
        self.assertEqual({scenario for _, scenario in cells}, {scenario for _, scenario in common.MATRIX_SUITES[SUITE]})
        self.assertEqual(set(common.SUITES[COMMISSION]), set(common.MATRIX_COMMISSIONING))

    def test_the_runner_accepts_both_selections(self):
        manifest = matrix.run.load_manifest(ROOT / "benchmarks/matrix-cases.json")
        for suite in (SUITE, COMMISSION):
            with self.subTest(suite=suite):
                cells = common.MATRIX_SUITES[suite]
                selected = matrix.selection(manifest, [case + ":" + scenario for case, scenario in cells])
                self.assertEqual([(case["id"], scenario) for case, scenario in selected], list(cells))

    def test_the_suites_are_appended_and_everything_before_them_is_what_it_was(self):
        self.assertEqual(common.MATRIX_REFERENCE_SUITES, (SUITE, COMMISSION))
        self.assertEqual(common.DURABLE_SUITES[49:], (SUITE, COMMISSION))
        earlier = {version: contract for version, contract in common._RECORDED_DURABLE_CONTRACTS.items()
                   if not version.startswith((SUITE + "-plan-", COMMISSION + "-plan-"))}
        self.assertEqual((len(earlier), len(common._RECORDED_DURABLE_CONTRACTS)), (52, 54))
        self.assertEqual(digest(earlier), "5d8ed74262952849f0a68fcd8677a78a5b6b165628616c514afff11545ab31c5")
        suites = {suite: [common.MATRIX_SUITES[suite], common.durable_plan_for_suite(suite),
                          common.matrix_contract(suite)]
                  for suite in common.MATRIX_SUITES if suite not in (SUITE, COMMISSION)}
        self.assertEqual(len(suites), 44)
        self.assertEqual(digest([list(common.DURABLE_SUITES[:49]), suites]),
                         "3535ce705f12f7b231b55d583d9a6464f66cde4c1cc72c4f5dded4e1ec5dedfb")

    def test_the_run_has_twelve_hours_and_its_commissioning_the_ordinary_allocation(self):
        skipped = ("suite", "plan", "binary_names", "binaries")
        contract = common.durable_plan_contract(common.durable_plan_for_suite(SUITE))
        longest = common.durable_plan_contract(common.durable_plan_for_suite(
            "matrix-all-local+tools+remote+tools+nfs+tools+memory+tools"))
        self.assertEqual({key: contract[key] for key in contract if key not in skipped},
                         {key: longest[key] for key in longest if key not in skipped})
        self.assertEqual((contract["max_runtime"], contract["runtime_profile"], contract["reservation_microusd"]),
                         (43200, "all-filesets-12h-v1", 65_000_000))
        self.assertEqual(contract["plan"], dict(longest["plan"], version=SUITE + "-plan-v1"))
        ordinary = common.durable_plan_contract(common.durable_plan_for_suite("matrix-commission-local+remote"))
        small = common.durable_plan_contract(common.durable_plan_for_suite(COMMISSION))
        self.assertEqual({key: small[key] for key in small if key not in skipped},
                         {key: ordinary[key] for key in ordinary if key not in skipped})
        self.assertEqual(small["plan"], dict(ordinary["plan"], version=COMMISSION + "-plan-v1"))
        for value in (contract, small):
            # no other tool of the suite is timed, so none is sent
            self.assertEqual(value["binary_names"], ("rcp", "rcpd", "filegen"))
        versions = [common.durable_plan_for_suite(suite)["version"] for suite in common.DURABLE_SUITES]
        self.assertEqual(len(set(versions)), len(versions))

    def test_the_suites_measure_as_the_others_do_and_name_the_limit_of_a_reference_copy(self):
        for suite in (SUITE, COMMISSION):
            with self.subTest(suite=suite):
                self.assertEqual(common.matrix_contract(suite), dict(
                    common.matrix_contract("matrix-all-local"), cells=common.MATRIX_SUITES[suite],
                    purpose="commissioning" if suite == COMMISSION else "performance", reference_limit=10))
                self.assertFalse(common.SUITE_PROFILES[suite]["active"])
        for suite in common.MATRIX_SUITES:
            if suite not in (SUITE, COMMISSION):
                self.assertNotIn("reference_limit", common.matrix_contract(suite), suite)
                self.assertFalse({scenario for _, scenario in common.MATRIX_SUITES[suite]}
                                 & set(common.MATRIX_REFERENCE_CELLS), suite)


class ReferenceAdmissionTests(unittest.TestCase):
    def test_by_hand_the_run_needs_the_twelve_hour_approval_and_its_commissioning_the_ordinary_time(self):
        named = by_hand(manual_suites=(COMMISSION, SUITE))
        result = approve(named, SUITE)
        self.assertEqual((result["runtime_seconds"], result["capacity_seconds"], result["reserve_microusd"]),
                         (43_200, 7_200, 65_000_000))
        small = approve(named, COMMISSION)
        self.assertEqual((small["runtime_seconds"], small["reserve_microusd"]), (10_800, 20_000_000))
        with self.assertRaisesRegex(ValueError, "extended manual qualification is not explicitly approved"):
            by_hand(manual_suites=(SUITE,), manual_runtime_seconds=0)
        with self.assertRaisesRegex(ValueError, "exactly its recorded runtime profile"):
            by_hand(manual_suites=(SUITE,), manual_runtime_seconds=28_800)
        with self.assertRaisesRegex(ValueError, "extended manual admission window is closed"):
            approve(by_hand(manual_suites=(SUITE,), extended_manual_not_after=ADMITTED.isoformat()), SUITE)

    def test_it_is_not_named_by_hand_next_to_the_weekly_suite_and_can_be_next_to_a_schedule(self):
        # eight hours by hand and twelve hours by hand are two policies
        with self.assertRaisesRegex(ValueError, "does not fit its explicit qualification budget"):
            by_hand(manual_suites=(SUITE, WEEKLY))
        with self.assertRaisesRegex(ValueError, "exactly its recorded runtime profile"):
            by_hand(manual_suites=(SUITE, WEEKLY), manual_runtime_seconds=28_800)
        # a scheduled run has a runtime of its own, so the policy that schedules one can name this suite
        both = dict(manual_suites=(SUITE,), manual_runtime_seconds=43_200,
                    extended_manual_not_after=(ADMITTED + timedelta(minutes=45)).isoformat())
        self.assertEqual(approve(weekly(**both), "matrix-all-local", event_name="schedule")["runtime_seconds"], 28_800)
        self.assertEqual(approve(weekly(**both), SUITE)["runtime_seconds"], 43_200)


class ReferenceWorkerTests(unittest.TestCase):
    def arguments(self, suite, other=None):
        argv = worker.command(dict(suite=suite, baseline="a" * 40), 9000, other)
        return matrix._arguments(argv[argv.index("benchmarks.matrix") + 1:]), argv

    def test_the_runner_is_given_the_tools_of_the_image_and_the_limit_of_the_suite(self):
        for suite in (SUITE, COMMISSION):
            with self.subTest(suite=suite):
                args, argv = self.arguments(suite, dict(nfs=dict(layout="nfs-loopback-v1")))
                self.assertEqual((args.cp, args.rsync), (Path("/usr/bin/cp"), Path("/usr/bin/rsync")))
                self.assertEqual(args.reference_limit, 10)
                # the suite's limit is said to the runner, whatever the runner's default is that day
                self.assertEqual(argv[argv.index("--reference-limit") + 1], str(common.matrix_contract(suite)["reference_limit"]))
                self.assertEqual(args.cells, [case + ":" + scenario for case, scenario in common.MATRIX_SUITES[suite]])
                # rsync pulls through the private sshd of the remote cells, on the unit's own loopback
                self.assertEqual(args.sshd, Path("/usr/sbin/sshd"))
                self.assertEqual(properties(argv)["RestrictAddressFamilies"], "AF_UNIX AF_INET")
                self.assertEqual((args.nfs_source_root, args.nfs_destination_root),
                                 (worker.DATA / "nfs-source", worker.DATA / "nfs-destination"))
                # the limit that catches hangs is the one of every matrix run
                self.assertEqual((args.timeout, args.repetitions), (3600, 3))
        # the tools are where the image has them, which is where a session of the private sshd finds them
        self.assertEqual(worker.REFERENCE_TOOLS, ("--cp", "/usr/bin/cp", "--rsync", "/usr/bin/rsync"))
        self.assertIn("/usr/bin", next(item for item in worker.ENVIRONMENT if item.startswith("--setenv=PATH=")))

    def test_a_suite_without_reference_cells_is_started_as_before(self):
        for suite in (WEEKLY, "matrix-commission-local+tools+remote+tools+nfs+tools+memory+tools", "matrix-all-local"):
            argv = worker.command(dict(suite=suite, baseline="a" * 40), 9000)
            self.assertEqual([word for word in argv if word.startswith(("--cp", "--rsync", "--reference"))], [], suite)

    def test_the_image_has_rsync(self):
        installer = (ROOT / "infra/aws_benchmark/install-worker.sh").read_text()
        packages = next(line for line in installer.splitlines() if "apt-get install" in line)
        self.assertIn(" rsync ", packages + " ")
        # cp is part of the base system: nothing is installed for it
        self.assertNotIn(" coreutils ", packages + " ")
        # the build stops when either is not where the worker names it
        for path in worker.REFERENCE_TOOLS[1::2]:
            self.assertIn("\ntest -x " + path + "\n", installer)
            self.assertLess(installer.index("apt-get install"), installer.index("test -x " + path))


class ReferenceRecordTests(unittest.TestCase):
    collect = test_matrix_results.MatrixCollectionTests.collect

    def test_a_suite_with_reference_cells_has_a_record_version_of_its_own(self):
        for suite, cells in common.MATRIX_SUITES.items():
            with self.subTest(suite=suite):
                schema = projection.matrix_schema(scenario for _, scenario in cells)
                self.assertEqual(schema == V7, suite in (SUITE, COMMISSION))
        self.assertEqual(projection.MATRIX_SCHEMAS, (V4, V5, V6, V7))
        self.assertEqual(projection.REFERENCE_SCHEMA, V7)
        # the versions before it are frozen
        frozen = {name: contract for name, contract in projection.PUBLIC_CONTRACTS.items() if name != V7}
        self.assertEqual(digest(frozen), "d46e2160c6ed70e71db840f9fdefc686cdf6411fe2289af50d995d98416f184a")

    def test_the_published_variants_are_the_ones_the_runner_runs(self):
        contract = projection.PUBLIC_CONTRACTS[V7]
        self.assertEqual(contract["tool_variants"],
                         {scenario: cell["variant"]["id"] for scenario, cell in (*matrix.TOOLS.items(),
                                                                                 *matrix.REFERENCES.items())})
        self.assertEqual(contract["reference_cells"], tuple(matrix.REFERENCES))
        earlier = projection.PUBLIC_CONTRACTS[V6]
        self.assertEqual({key: contract[key] for key in ("series_contract", "verification", "variant")},
                         {key: earlier[key] for key in ("series_contract", "verification", "variant")})
        self.assertEqual(contract["validation"],
                         "Not verified: a copy or a tool run counts when its command exits cleanly and reports the "
                         "expected counts. A copy by cp or rsync, which report none, counts when it exits cleanly.")

    def test_a_run_publishes_each_reference_cell_under_what_its_tool_ran(self):
        result, report, public = self.collect(COMMISSION, matrix_record(COMMISSION))
        self.assertEqual((result["state"], result["retired"], report["benchmark_valid"]), ("success", True, True),
                         public["diagnostics"])
        self.assertEqual(public["schema"], V7)
        self.assertEqual([(cell["case_id"], cell["scenario"], cell["outcome"]) for cell in public["cells"]],
                         [(case, scenario, "complete") for case, scenario in common.MATRIX_SUITES[COMMISSION]])
        ran = {(item["case_id"], item["scenario"]): item["variant_id"] for item in public["measurements"]}
        self.assertEqual([ran["commission-10k", scenario] for scenario in (*ARRAYS, *MOUNTS)],
                         ["rcp-default", "cp-a", "rsync-a", "rcp-default", "rsync-a", "rcp-default", "cp-a", "rsync-a"])
        # each cell is a line of its own: rsync between two directories is not rsync from a host
        self.assertEqual(len({item["series_id"] for item in public["measurements"]}), 13)
        # every cell of this run is a copy: no tool of the suite is timed on one
        self.assertEqual(public["note"], "Each measurement is the median of 3 copies.")
        self.assertEqual(public["validation"], projection.PUBLIC_CONTRACTS[V7]["validation"])
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)), "13 of 13 measurements completed")

    def test_a_record_of_this_version_with_tool_cells_says_that_it_has_tool_runs(self):
        # no suite has both today; the version allows it, and the note follows the cells of the run
        raw = matrix_record(COMMISSION)
        value = projection.freeze(
            "repo-655073755-run-43200000001",
            dict(run="43200000001-1", run_url="https://github.com/wykurz/rcp/actions/runs/43200000001/attempts/1",
                 suite=COMMISSION, baseline="1" * 40, candidate="1" * 40, controller="2" * 40),
            dict(ami="ami-0123456789abcdef0", source="s" * 40), True,
            dict(controller=dict(request=None), errors=[], expected_verification=dict(policy="exit-status-summary-v1"),
                 cells=[dict(case_id=cell["case_id"], scenario=cell["scenario"], outcome="complete", stage=None,
                             message=None) for cell in raw["cells"]]),
            raw, collected_at="2026-11-20T18:44:32+00:00")
        self.assertEqual(value["note"], "Each measurement is the median of 3 copies.")
        with mock.patch.dict(projection.PUBLIC_CONTRACTS[V7], reference_cells=("local-cp",)):
            # were only one of these cells a reference cell, the others would be runs of a tool
            again = projection.freeze(
                "repo-655073755-run-43200000001",
                dict(run="43200000001-1", run_url="https://github.com/wykurz/rcp/actions/runs/43200000001/attempts/1",
                     suite=COMMISSION, baseline="1" * 40, candidate="1" * 40, controller="2" * 40),
                dict(ami="ami-0123456789abcdef0", source="s" * 40), True,
                dict(controller=dict(request=None), errors=[], expected_verification=dict(policy="exit-status-summary-v1"),
                     cells=[dict(case_id=cell["case_id"], scenario=cell["scenario"], outcome="complete", stage=None,
                                 message=None) for cell in raw["cells"]]),
                raw, collected_at="2026-11-20T18:44:32+00:00")
        self.assertEqual(again["note"], "Each measurement is the median of 3 copies or tool runs.")

    def test_the_copies_by_rcp_stay_on_the_lines_they_had(self):
        # a point of this run continues the trend of the same copy in every other suite
        reference = self.collect(COMMISSION, matrix_record(COMMISSION))[2]
        everywhere = "matrix-commission-local+tools+remote+tools+nfs+tools+memory+tools"
        before = self.collect(everywhere, matrix_record(everywhere))[2]
        lines = {(item["case_id"], item["scenario"]): item["series_id"] for item in before["measurements"]}
        shared = [item for item in reference["measurements"] if item["scenario"] in matrix.COPIES]
        self.assertEqual(len(shared), 5)
        for item in shared:
            self.assertEqual(lines[item["case_id"], item["scenario"]], item["series_id"])

    def test_a_copy_that_was_cut_off_is_published_as_timed_out_and_the_run_still_counts(self):
        cut = {index(COMMISSION, "commission-10k", "nfs-cp"): dict(CUT, message=CUT["message"].replace("local", "nfs")),
               index(COMMISSION, "commission-10k", "remote-rsync"): dict(CUT, message=CUT["message"].replace(
                   "local", "remote"))}
        result, report, public = self.collect(COMMISSION, matrix_record(COMMISSION, cut))
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        published = {(cell["case_id"], cell["scenario"]): cell for cell in public["cells"]}
        self.assertEqual({key: published["commission-10k", "nfs-cp"][key] for key in ("outcome", "stage", "message")},
                         dict(outcome="timed_out", stage="copy", message=cut[index(COMMISSION, "commission-10k",
                                                                                   "nfs-cp")]["message"]))
        self.assertEqual(len(public["measurements"]), 11)
        bound = projection.bind(public, "a" * 64)
        self.assertEqual(projection.outcome_label(bound), "11 of 13 measurements completed")
        page = export.render(bound)
        self.assertIn("<td>timed out</td><td>—</td><td>—</td><td>—</td><td class='reason'>During copy: copy 1 did not "
                      "finish within 6002.0 s, 10 times the median of the nfs cell</td>", page)

    def test_a_run_that_gave_another_limit_than_its_suite_is_not_the_suites(self):
        for limit in (5, 10.5):
            with self.subTest(limit=limit):
                result, report, public = self.collect(COMMISSION, matrix_record(COMMISSION, reference_limit=limit))
                self.assertEqual((result["state"], report["benchmark_valid"]), ("failed", False))
                # nothing that such a document says of itself is published
                self.assertEqual((public["cells"], public["measurements"]), ([], []))
                self.assertEqual(public["diagnostics"], ["qualification: unexpected matrix measurement contract"])

    def test_the_run_of_the_suite_is_collected_with_its_twelve_hours(self):
        result, report, public = self.collect(SUITE, matrix_record(SUITE), **TWELVE_HOURS)
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        self.assertEqual((public["schema"], len(public["cells"]), len(public["measurements"])), (V7, 18, 18))


def sampled_reference(suite):
    """A run of a reference suite with every phase sampled: no rrm removes its copies, so each has its removal."""
    raw, total = sampled_matrix(suite, diagnostics.TELEMETRY_DESCRIPTORS[-1])
    cases = [item["case"]["id"] for item in raw["file_sets"]]
    for cell in raw["cells"]:
        for item in cell["copies"]:
            phases = item["system_telemetry"]["phases"]
            phases.append(sampled_phase("cleanup", cases.index(cell["case_id"]), item["trial_index"]))
            item["system_telemetry"]["samples"] = 20 * len(phases)
            total += 1
    matrix.validate(raw)
    return raw, total


class ReferenceDiagnosticsTests(unittest.TestCase):
    def test_a_copy_by_another_tool_names_what_it_ran_and_has_no_figures(self):
        record, extra = companion(COMMISSION)
        self.assertEqual((extra["schema"], extra["reason"]), (D6, None))
        self.assertEqual(len(extra["trials"]), 39)
        by_scenario = {}
        for trial in extra["trials"]:
            by_scenario.setdefault(trial["scenario"], []).append(trial)
        self.assertEqual({scenario: {trial["variant_id"] for trial in trials} for scenario, trials in by_scenario.items()},
                         {"local": {"rcp-default"}, "remote": {"rcp-default"}, "nfs": {"rcp-default"},
                          "local-cp": {"cp-a"}, "nfs-cp": {"cp-a"}, "local-rsync": {"rsync-a"},
                          "remote-rsync": {"rsync-a"}, "nfs-rsync": {"rsync-a"}})
        for scenario, trials in by_scenario.items():
            for trial in trials:
                reference = scenario in REFERENCES
                self.assertEqual((trial["status"], trial["metrics"] is None),
                                 ("unavailable", True) if reference else ("complete", False), scenario)
                # the pull by rsync is no copy by rcpd: only rcp's remote copy names two daemons
                self.assertEqual(trial["roles"] is not None, scenario == "remote", scenario)
        # nothing is missing that such a run could have: a copy by cp or rsync has no figures to give
        self.assertEqual(extra["status"], "complete")
        self.assertEqual(extra["process_scope"], diagnostics.REFERENCE_PROCESS_SCOPE)
        self.assertEqual(extra["process_scope"], diagnostics.STORAGE_PROCESS_SCOPE +
                         " A copy by cp or rsync has no figures: neither tool reports any about itself.")
        diagnostics.validate(extra, record)

    def test_figures_that_a_record_holds_for_a_copy_by_cp_or_rsync_are_not_published(self):
        record, _ = matrix_example(COMMISSION)
        raw = matrix_record(COMMISSION, environment=test_matrix_diagnostics.ENVIRONMENT)
        figures = raw["cells"][0]["copies"][0]["resources"]
        for cell in raw["cells"]:
            if cell["scenario"] in REFERENCES:
                for item in cell["copies"]:
                    item["resources"] = json.loads(json.dumps(figures))
        extra = diagnostics.derive(record, b"archive", raw, dict(test_matrix_diagnostics.WORKER, other_storage=PREPARED))
        self.assertEqual(len(extra["trials"]), 39)
        self.assertEqual({trial["status"] for trial in extra["trials"] if trial["scenario"] in REFERENCES}, {"unavailable"})

    def test_a_copy_by_rcp_without_figures_still_leaves_the_document_partial(self):
        record, _ = matrix_example(COMMISSION)
        raw = matrix_record(COMMISSION, environment=test_matrix_diagnostics.ENVIRONMENT)
        del raw["cells"][0]["copies"][0]["resources"]
        extra = diagnostics.derive(record, b"archive", raw, dict(test_matrix_diagnostics.WORKER, other_storage=PREPARED))
        self.assertEqual(extra["status"], "partial")

    def test_the_cp_and_the_rsync_of_the_run_are_published(self):
        record, extra = companion(COMMISSION)
        self.assertEqual(extra["reference_tools"], REFERENCE_VERSIONS)
        self.assertEqual(extra["other_storage"]["nfs"]["status"], "prepared")
        # what a tool says of itself is shown as it is when it is short plain text, and not at all otherwise
        raw = matrix_record(COMMISSION, environment=test_matrix_diagnostics.ENVIRONMENT)
        for said, published in (("rsync 3.4.1", "rsync 3.4.1"), ("x" * 128, "x" * 128), ("x" * 129, None),
                                ("two\nlines", None), ("caf\u00e9", None), ("<b>3</b>", "<b>3</b>")):
            raw["tools"]["rsync"]["version"] = said
            derived = diagnostics.derive(record, b"archive", raw, dict(test_matrix_diagnostics.WORKER,
                                                                       other_storage=PREPARED))
            self.assertEqual(derived["reference_tools"].get("rsync"), published, said)
            self.assertEqual(derived["reference_tools"]["cp"], REFERENCE_VERSIONS["cp"])
            self.assertEqual(len(derived["trials"]), 39)

    def test_the_companion_is_checked_against_its_record(self):
        record, extra = companion(COMMISSION)
        position = next(number for number, trial in enumerate(extra["trials"]) if trial["scenario"] == "local-cp")
        figures = next(trial["metrics"] for trial in extra["trials"] if trial["scenario"] == "local")
        for change in (lambda value: value["trials"][position].update(variant_id="rcp-default"),
                       lambda value: value["trials"][position].update(variant_id="rsync-a"),
                       # a copy by cp has no figures, whatever a document says
                       lambda value: value["trials"][position].update(status="complete", metrics=dict(figures)),
                       lambda value: value["trials"][position].update(roles=dict(source=None, destination=None)),
                       lambda value: value.pop("reference_tools"),
                       lambda value: value.update(reference_tools=None),
                       lambda value: value["reference_tools"].update(tar="tar 1"),
                       lambda value: value["reference_tools"].update(cp="two\nlines"),
                       lambda value: value["reference_tools"].update(cp="x" * 129),
                       lambda value: value.pop("other_storage"),
                       lambda value: value.update(schema=D5),
                       lambda value: value.update(process_scope=diagnostics.STORAGE_PROCESS_SCOPE),
                       lambda value: value.update(status="partial")):
            broken = json.loads(json.dumps(extra))
            change(broken)
            with self.subTest(change=change), self.assertRaises(ValueError):
                diagnostics.validate(broken, record)

    def test_the_version_takes_the_tool_cells_of_the_version_before_it(self):
        # no suite times a tool of the suite and copies with another one, and a document of this version may hold both
        self.assertEqual(diagnostics._tool_cells(D6), diagnostics._tool_cells(D5))
        self.assertEqual([diagnostics._variant(D6, scenario) for scenario in ("nfs", "nfs-rrm", "nfs-cp", "remote-rsync")],
                         ["rcp-default", "rrm-default", "cp-a", "rsync-a"])
        # the version before knows no such copy: there it would be one more copy by rcp
        self.assertEqual(diagnostics._variant(D5, "nfs-cp"), "rcp-default")

    def test_earlier_companions_keep_their_version_and_have_no_such_part(self):
        record, extra = companion("matrix-commission-local+tools+remote+tools+nfs+tools+memory+tools")
        self.assertEqual(extra["schema"], D5)
        self.assertNotIn("reference_tools", extra)
        with self.assertRaises(ValueError):
            diagnostics.validate(dict(extra, reference_tools={}), record)

    def test_a_run_without_usable_diagnostics_still_has_a_companion(self):
        record, _ = matrix_example(COMMISSION)
        for extra in (diagnostics.unavailable(record), diagnostics.derive(record, b"archive", {}, {})):
            self.assertEqual((extra["schema"], extra["status"], extra["trials"]), (D6, "unavailable", []))
            self.assertEqual((extra["reference_tools"], extra["other_storage"]), ({}, dict(nfs=dict(status="not_recorded"))))
            diagnostics.validate(extra, record)
            self.assertIn("No per-copy process-resource measurements are available.", diagnostics.render(extra, record))

    def test_a_cell_that_was_cut_off_keeps_its_one_copy_in_the_companion(self):
        cut = {index(COMMISSION, "commission-10k", "nfs-cp"): CUT}
        record, extra = companion(COMMISSION, outcomes=cut)
        self.assertEqual(sum(trial["scenario"] == "nfs-cp" for trial in extra["trials"]), 1)
        self.assertEqual(extra["status"], "complete")

    def test_the_run_page_names_cp_and_rsync_and_says_that_they_report_nothing(self):
        record, extra = companion(COMMISSION)
        html = diagnostics.render(extra, record)
        for case, scenario, process in (("commission-10k", "local-cp", "cp"), ("commission-10k", "nfs-rsync", "rsync"),
                                        ("commission-64m", "remote-rsync", "rsync")):
            self.assertIn("<td><code>" + case + "</code></td><td><code>" + scenario + "</code></td><td>1</td><td>" +
                          process + "</td><td>not reported</td>" + "<td>—</td>" * 7 + "</tr>", html)
        self.assertIn("<td><code>commission-10k</code></td><td><code>nfs</code></td><td>1</td><td>rcp</td>"
                      "<td>complete</td>", html)
        self.assertNotIn("<td>rsync</td><td>unavailable</td>", html)
        self.assertNotIn("<td><code>remote-rsync</code></td><td>1</td><td>source rcpd</td>", html)
        self.assertIn("<p>The reference copies of this run were made by <code>cp (GNU coreutils) 9.4</code> and "
                      "<code>rsync  version 3.2.7  protocol version 31</code>.</p>", html)
        # what a tool says of itself is text, never markup
        tagged = dict(extra, reference_tools=dict(cp="<b>3</b>"))
        self.assertIn("were made by <code>&lt;b&gt;3&lt;/b&gt;</code>.</p>", diagnostics.render(tagged, record))
        self.assertIn("Process-resource availability: complete.", html)
        # a copy is a copy, whoever made it
        sampled, _ = sampled_reference(COMMISSION)
        html = diagnostics.render(diagnostics.derive(record, b"archive", sampled, dict(
            test_matrix_diagnostics.WORKER, other_storage=PREPARED)), record)
        self.assertIn("Sampled phases: commission-10k / nfs-cp / copy 3", html)
        self.assertNotIn("nfs-cp / run", html)

    def test_a_run_that_recorded_no_tool_names_none(self):
        record, _ = matrix_example(COMMISSION)
        raw = matrix_record(COMMISSION, environment=test_matrix_diagnostics.ENVIRONMENT)
        for name in ("cp", "rsync"):
            raw["tools"][name]["version"] = "\x00"
        extra = diagnostics.derive(record, b"archive", raw, dict(test_matrix_diagnostics.WORKER, other_storage=PREPARED))
        self.assertEqual(extra["reference_tools"], {})
        self.assertNotIn("The reference copies of this run were made by", diagnostics.render(extra, record))

    def test_the_time_of_a_run_counts_the_copies_of_each_tool(self):
        raw, total = sampled_reference(COMMISSION)
        record, _ = matrix_example(COMMISSION)
        extra = diagnostics.derive(record, b"archive", raw, dict(test_matrix_diagnostics.WORKER, other_storage=PREPARED))
        use = diagnostics.time_use(extra, record)
        seconds = 612.345678
        # cp made the copies of three cells and rsync of five, three times each
        self.assertEqual({key: round(value / seconds) for key, value in use["commands"].items()},
                         dict(local=6, remote=6, nfs=3, cp=9, rsync=15))
        # every copy is removed by the run, whoever made it
        self.assertEqual(round(use["around"]["copies"] / seconds), 39)
        self.assertNotIn("links", use["around"])
        self.assertTrue(use["complete"])
        self.assertAlmostEqual(use["total"], total * seconds, places=3)

    def test_the_sampled_detail_of_the_run_fits_the_journal_and_the_public_document(self):
        descriptor = diagnostics.TELEMETRY_DESCRIPTORS[-1]
        self.assertEqual(descriptor, telemetry.descriptor(telemetry.MATRIX_LIMITS))
        raw, total = sampled_reference(SUITE)
        # four file sets; a cache step, a command and a removal for each of 54 copies
        self.assertEqual(total, 13 + 54 * 3)
        self.assertLess(total, descriptor["max_phases"] - 64)
        record, _ = matrix_example(SUITE)
        extra = diagnostics.derive(record, b"archive", raw, dict(test_matrix_diagnostics.WORKER, other_storage=PREPARED))
        self.assertEqual((extra["status"], extra["reason"], len(extra["trials"]), len(extra["cases"])),
                         ("complete", None, 54, 4))
        self.assertLessEqual(len(extra["trials"]), diagnostics.MAX_TRIALS)
        self.assertLess(len(json.dumps(extra, separators=(",", ":"))), diagnostics.MAX_DOCUMENT * 3 // 4)
        self.assertLess(len(diagnostics.render(extra, record).encode()), projection.MAX_PUBLIC_HTML // 4)


if __name__ == "__main__":
    unittest.main()
