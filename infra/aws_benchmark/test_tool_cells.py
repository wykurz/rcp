"""Tool cells on the worker: the suites that select them and the binaries those suites need."""

import copy
import json
from pathlib import Path
import re
import tempfile
import unittest

import yaml

from benchmarks import matrix
from . import common, production_client, production_diagnostics as diagnostics, production_projection as projection
from . import results, test_matrix_diagnostics, test_matrix_results, test_production_stack as stack, worker
from .test_matrix_pages import matrix_example
from .test_matrix_results import matrix_record
from .production_admission import staged_key
from .production_mailbox import MailboxPaths
from .production_policy import manifest as checked_manifest
from .test_durable_worker import durable_fixture, request as worker_request
from .test_production_admission import Cloud
from .test_production_policy import identity, inputs, policy
from .test_production_runtime import ProductionCloud
from .test_worker_review import report

ROOT = Path(__file__).resolve().parents[2]
FILE_SETS = ("bulk-1g-1tb", "bulk-100m-1024gb", "bulk-1m-1024gb", "tiny-1k-10240k")
SELECTIONS = ("all", "1g", "100m", "1m", "1k", "commission")
TOOLS = ("rcmp", "rlink", "rrm")
EARLIER = ("local", "remote", "local+remote")
WITH_TOOLS = ("local+tools", "local+tools+remote", "local+tools+remote+tools")
AFTER_LOCAL = tuple("local-" + tool for tool in TOOLS)
AFTER_REMOTE = tuple("remote-" + tool for tool in TOOLS)
# the suite with the most cells: every file set, both kinds of copy, and the tools after each
WEEKLY = "matrix-all-local+tools+remote+tools"


def properties(argv):
    return dict(item[len("--property="):].split("=", 1) for item in argv if item.startswith("--property="))


class ToolSuiteTests(unittest.TestCase):
    def test_the_tools_are_the_ones_the_runner_times_in_the_order_it_runs_them(self):
        self.assertEqual(common.MATRIX_TOOLS, tuple(matrix.TOOL_RUNS))
        self.assertEqual(common.MATRIX_TOOLS, TOOLS)
        # each tool is its own program, and a cell is named after the copies it follows and that program
        self.assertEqual([run["variant"]["tool"] for run in matrix.TOOL_RUNS.values()], list(TOOLS))
        self.assertEqual(tuple(matrix.TOOLS)[:6], (*AFTER_LOCAL, *AFTER_REMOTE))
        self.assertEqual(common.MATRIX_TOOL_CELLS, frozenset((*AFTER_LOCAL, *AFTER_REMOTE)))
        # the runner also times the tools after copies on other storage, which no suite selects yet
        self.assertLess(common.MATRIX_TOOL_CELLS, frozenset(matrix.TOOLS))

    def test_every_selection_has_suites_with_the_tools_after_local_copies_or_after_both(self):
        self.assertEqual(sorted(common.MATRIX_SUITES), sorted(
            f"matrix-{selection}-{scenarios}" for selection in SELECTIONS for scenarios in (*EARLIER, *WITH_TOOLS)))
        # a suite name reads as the order of the cells of each file set: the tools of a copy cell
        # come right after it
        self.assertEqual(common.MATRIX_SUITES["matrix-1k-local+tools"], tuple(
            ("tiny-1k-10240k", scenario) for scenario in ("local", *AFTER_LOCAL)))
        self.assertEqual(common.MATRIX_SUITES["matrix-all-local+tools+remote"], tuple(
            (case, scenario) for case in FILE_SETS for scenario in ("local", *AFTER_LOCAL, "remote")))
        self.assertEqual(common.MATRIX_SUITES[WEEKLY], tuple(
            (case, scenario) for case in FILE_SETS for scenario in ("local", *AFTER_LOCAL, "remote", *AFTER_REMOTE)))
        self.assertEqual(len(common.MATRIX_SUITES[WEEKLY]), 32)
        self.assertEqual(common.MATRIX_SUITES["matrix-commission-local+tools"], tuple(
            (case, scenario) for case in ("commission-64m", "commission-10k") for scenario in ("local", *AFTER_LOCAL)))
        self.assertEqual(common.SUITES[WEEKLY], FILE_SETS)
        manifest = matrix.run.load_manifest(ROOT / "benchmarks/matrix-cases.json")
        for suite, cells in common.MATRIX_SUITES.items():
            with self.subTest(suite=suite):
                # a suite name says whether it times the tools
                self.assertEqual(any(scenario in matrix.TOOLS for _, scenario in cells), "+tools" in suite)
                self.assertLessEqual({scenario for _, scenario in cells}, set(matrix.SCENARIOS))
                # and the runner accepts its selection: every tool cell has the copy cell it follows
                selected = matrix.selection(manifest, [case + ":" + scenario for case, scenario in cells])
                self.assertEqual([(case["id"], scenario) for case, scenario in selected], list(cells))

    def test_the_suites_of_earlier_releases_keep_their_place_and_their_plans(self):
        earlier = [f"matrix-{selection}-{scenarios}" for scenarios in EARLIER for selection in SELECTIONS]
        self.assertEqual(common.DURABLE_SUITES[5:23], tuple(earlier))
        self.assertEqual(common.DURABLE_SUITES[23:], tuple(
            f"matrix-{selection}-{scenarios}" for scenarios in WITH_TOOLS for selection in SELECTIONS))
        self.assertEqual(len(common.DURABLE_SUITES), 41)
        for suite in earlier:
            with self.subTest(suite=suite):
                contract = common.durable_plan_contract(common.durable_plan_for_suite(suite))
                self.assertEqual(contract["plan"]["version"], suite + "-plan-v1")
                self.assertEqual(contract["binary_names"], ("rcp", "rcpd", "filegen"))
                self.assertEqual(common.suite_binaries(suite), common.BINARIES)

    def test_a_suite_with_tools_needs_their_binaries_and_no_other_suite_does(self):
        for suite in common.DURABLE_SUITES:
            with self.subTest(suite=suite):
                names = ("rcp", "rcpd", "filegen", *(TOOLS if "+tools" in suite else ()))
                contract = common.durable_plan_contract(common.durable_plan_for_suite(suite))
                self.assertEqual(contract["binary_names"], names)
                self.assertEqual(common.suite_binaries(suite), tuple(
                    role + "/" + name for role in ("baseline", "candidate") for name in names))
                self.assertLessEqual(set(common.suite_binaries(suite)), set(common.KNOWN_BINARIES))
        self.assertEqual(common.KNOWN_BINARIES, tuple(
            role + "/" + name for role in ("baseline", "candidate") for name in ("rcp", "rcpd", "filegen", *TOOLS)))
        # the binaries of the first protocols are a fixed name that earlier readers rely on
        self.assertEqual(common.BINARIES, tuple(
            role + "/" + name for role in ("baseline", "candidate") for name in ("rcp", "rcpd", "filegen")))
        with self.assertRaisesRegex(ValueError, "not allowlisted"):
            common.suite_binaries("matrix-all-local+tools+wan")

    def test_only_the_time_of_every_file_set_together_grows_with_the_tools(self):
        # every file set with the tools has twelve hours where it had eight (test_tool_allocation.py)
        longer = dict(max_runtime=43200, runtime_profile="all-filesets-12h-v1", reservation_microusd=65_000_000)
        for selection in SELECTIONS:
            plain = common.durable_plan_contract(common.durable_plan_for_suite(f"matrix-{selection}-local+remote"))
            for scenarios in WITH_TOOLS:
                suite = f"matrix-{selection}-{scenarios}"
                with self.subTest(suite=suite):
                    contract = common.durable_plan_contract(common.durable_plan_for_suite(suite))
                    skipped = ("suite", "plan", "binary_names", "binaries")
                    self.assertEqual({key: contract[key] for key in contract if key not in skipped},
                                     {key: plain[key] for key in plain if key not in skipped}
                                     | (longer if selection == "all" else {}))
                    # the twelve-hour plans are a second version: the first stays recorded
                    version = suite + ("-plan-v2" if selection == "all" else "-plan-v1")
                    self.assertEqual(contract["plan"], dict(plain["plan"], version=version))
                    self.assertEqual(common.matrix_contract(suite), dict(
                        common.matrix_contract(f"matrix-{selection}-local+remote"), cells=common.MATRIX_SUITES[suite]))
        versions = [common.durable_plan_for_suite(suite)["version"] for suite in common.DURABLE_SUITES]
        self.assertEqual(len(set(versions)), len(versions))

    def test_the_worker_passes_the_tool_cells_to_the_runner(self):
        argv = worker.command(dict(suite="matrix-commission-local+tools+remote+tools", baseline="a" * 40), 9000)
        args = matrix._arguments(argv[argv.index("benchmarks.matrix") + 1:])
        self.assertEqual(args.cells, [case + ":" + scenario for case in ("commission-64m", "commission-10k")
                                      for scenario in ("local", *AFTER_LOCAL, "remote", *AFTER_REMOTE)])
        self.assertEqual(args.bin_dir, worker.STATE / "candidate")
        # the tools open no socket, whichever copies they follow: only a suite with remote copies may
        with_remote = properties(argv)
        without = properties(worker.command(dict(suite="matrix-commission-local+tools", baseline="a" * 40), 9000))
        self.assertEqual((with_remote["RestrictAddressFamilies"], without["RestrictAddressFamilies"]),
                         ("AF_UNIX AF_INET", "AF_UNIX"))
        plain = properties(worker.command(dict(suite="matrix-commission-local", baseline="a" * 40), 9000))
        self.assertEqual(without, plain)
        self.assertEqual(with_remote, properties(worker.command(
            dict(suite="matrix-commission-local+remote", baseline="a" * 40), 9000)))


SUITE = "matrix-commission-local+tools"
PLAIN = "matrix-commission-local"
NAMES = ("rcp", "rcpd", "filegen", *TOOLS)


def tool_inputs(suite=SUITE, **updates):
    """What a client sends for a suite: its manifest, with an entry for each binary of the suite."""
    return inputs(suite=suite, binaries={key: dict(sha256="b" * 64, bytes=100)
                                         for key in common.suite_binaries(suite)}) | updates


class ToolBinaryTests(unittest.TestCase):
    """The worker runs what it is sent, so a suite with tool cells sends the tools along the whole chain."""

    def test_the_build_keeps_every_tool_and_the_workflow_offers_every_suite(self):
        workflow = yaml.load((ROOT / ".github/workflows/aws-benchmark-production.yml").read_text(),
                             Loader=yaml.BaseLoader)
        step = next(step for step in workflow["jobs"]["build"]["steps"]
                    if step.get("name") == "Build one immutable A/A artifact before admission")
        # one artifact serves every suite; the client sends the part of it that the suite needs
        self.assertEqual(re.search(r"for name in ([a-z ]+); do", step["run"])[1].split(), list(NAMES))
        self.assertIn("just build-release\n", step["run"])
        self.assertEqual(workflow["on"]["workflow_dispatch"]["inputs"]["suite"]["options"],
                         list(common.DURABLE_SUITES))

    def test_the_client_sends_the_binaries_of_the_suite_and_no_others(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in NAMES:
                (root / name).write_bytes(("test-only-" + name).encode())
            manifest, binaries = production_client.prepare(root, "a" * 40, suite=SUITE)
            self.assertEqual(sorted(binaries), sorted(common.suite_binaries(SUITE)))
            self.assertEqual(sorted(manifest["binaries"]), sorted(binaries))
            for name in NAMES:
                self.assertEqual(binaries["candidate/" + name], ("test-only-" + name).encode())
                self.assertEqual(binaries["baseline/" + name], binaries["candidate/" + name])
            self.assertEqual(checked_manifest(manifest)["suite"], SUITE)
            # a suite without tool cells sends what it always sent, whatever else was built
            plain, sent = production_client.prepare(root, "a" * 40, suite=PLAIN)
            self.assertEqual(sorted(sent), sorted(common.BINARIES))
            self.assertEqual(checked_manifest(plain)["suite"], PLAIN)
            # and a suite with them cannot be submitted without one of them
            (root / "rlink").unlink()
            with self.assertRaises((OSError, ValueError)):
                production_client.prepare(root, "a" * 40, suite=SUITE)
            self.assertEqual(sorted(production_client.prepare(root, "a" * 40, suite=PLAIN)[1]),
                             sorted(common.BINARIES))

    def test_a_manifest_holds_exactly_the_binaries_of_its_suite(self):
        self.assertEqual(set(checked_manifest(tool_inputs())["binaries"]), set(common.suite_binaries(SUITE)))
        self.assertEqual(len(common.suite_binaries(SUITE)), 12)
        for value in (
                # the tools without a suite that runs them, and a suite that runs them without the tools
                tool_inputs(suite=PLAIN, binaries=tool_inputs()["binaries"]),
                tool_inputs(suite=SUITE, binaries=inputs()["binaries"]),
                tool_inputs(binaries={key: entry for key, entry in tool_inputs()["binaries"].items()
                                      if key != "baseline/rrm"}),
                tool_inputs(binaries=tool_inputs()["binaries"] | {"candidate/rchm": dict(sha256="b" * 64, bytes=100)})):
            with self.subTest(suite=value["suite"], binaries=sorted(value["binaries"])), \
                    self.assertRaisesRegex(ValueError, "unexpected binary manifest"):
                checked_manifest(value)
        # both sides of a tool are the same bytes, as for every other binary
        for name in TOOLS:
            changed = tool_inputs()
            changed["binaries"]["candidate/" + name] = dict(sha256="d" * 64, bytes=100)
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "A/A executable bytes differ"):
                checked_manifest(changed)

    def test_admission_issues_an_upload_for_each_binary_of_the_suite_and_seals_them_all(self):
        cloud = Cloud()
        cloud.policy = policy(manual_suites=("bulk-1g-metadata", SUITE))
        cloud.ddb.items["policy"]["version"]["S"] = cloud.policy.version
        response = cloud.service.begin("not-a-real-token", tool_inputs())
        self.assertEqual(set(response["uploads"]), set(common.suite_binaries(SUITE)))
        keys = [call.kwargs["Params"]["Key"] for call in cloud.api["s3"].generate_presigned_url.call_args_list]
        self.assertEqual(sorted(keys), sorted(staged_key(response["run_key"], name)
                                              for name in common.suite_binaries(SUITE)))
        request = cloud.service.store(cloud.who).read().state["request"]
        self.assertEqual(set(request["sha256"]), set(common.suite_binaries(SUITE)))
        self.assertEqual(request["plan"], common.durable_plan_for_suite(SUITE))
        # a key is one of the binaries some plan names, under the run that staged it
        self.assertEqual(staged_key("repo-1-run-2", "candidate/rrm"), "production/staging/repo-1-run-2/candidate/rrm")
        for name in ("candidate/rchm", "rrm", "candidate/rrm/extra", "other/rrm"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "invalid immutable upload key"):
                staged_key("repo-1-run-2", name)

    def test_admission_refuses_a_manifest_that_is_not_the_suites(self):
        cloud = Cloud()
        cloud.policy = policy(manual_suites=("bulk-1g-metadata", SUITE, PLAIN))
        cloud.ddb.items["policy"]["version"]["S"] = cloud.policy.version
        for value in (tool_inputs(suite=SUITE, binaries=inputs()["binaries"]),
                      tool_inputs(suite=PLAIN, binaries=tool_inputs()["binaries"])):
            with self.subTest(suite=value["suite"]), self.assertRaisesRegex(ValueError, "unexpected binary manifest"):
                cloud.service.begin("not-a-real-token", value)
        cloud.api["s3"].generate_presigned_url.assert_not_called()

    def test_a_mailbox_holds_any_binary_that_a_plan_names(self):
        paths = MailboxPaths.for_instance("123456789012", "us-east-1", "i-" + "1" * 17)
        for name in common.KNOWN_BINARIES:
            self.assertEqual(paths.binary(name), paths.prefix + "input/" + name)
        for name in ("candidate/rchm", "rrm", "candidate/../rrm", "input/candidate/rrm"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "unexpected mailbox binary"):
                paths.binary(name)

    def worker_run(self, suite, sent):
        """Run the worker for a suite whose mailbox holds `sent`; return what it stored and reported."""
        with durable_fixture() as (root, cloud, measure, _, _, _):
            lease = json.loads(cloud.objects[common.WINDOW_KEY])
            lease.update(suite=suite, plan=common.durable_plan_for_suite(suite))
            request = worker_request(lease)
            request["sha256"] = dict.fromkeys(sent, common.digest(b"binary"))
            cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
            cloud.objects[common.REQUEST_KEY] = json.dumps(request).encode()
            for key in [key for key in cloud.objects if key.startswith("input/") and key != common.REQUEST_KEY]:
                del cloud.objects[key]
            cloud.objects.update({"input/" + key: b"binary" for key in sent})
            (root / "benchmarks/matrix-cases.json").write_bytes((ROOT / "benchmarks/matrix-cases.json").read_bytes())
            worker.run(cloud.api["s3"], "bucket", direct=True)
            metadata, _ = report(cloud)
            stored = sorted(str(path.relative_to(root / "state")) for path in (root / "state").glob("*/*"))
            return metadata, stored, measure

    def test_the_worker_fetches_the_binaries_of_its_plan(self):
        metadata, stored, measure = self.worker_run(SUITE, common.suite_binaries(SUITE))
        self.assertTrue(metadata["status"]["success"], metadata["status"])
        self.assertEqual(stored, sorted(common.suite_binaries(SUITE)))
        argv = measure.call_args.args[0]
        self.assertEqual([item for item in argv if item.endswith("-rrm")],
                         ["commission-64m:local-rrm", "commission-10k:local-rrm"])
        # a suite without tool cells asks for nothing more than before
        metadata, stored, _ = self.worker_run(PLAIN, common.BINARIES)
        self.assertTrue(metadata["status"]["success"], metadata["status"])
        self.assertEqual(stored, sorted(common.BINARIES))

    def test_the_worker_refuses_a_request_that_is_not_its_plans(self):
        for suite, sent in ((SUITE, common.BINARIES), (PLAIN, common.suite_binaries(SUITE))):
            with self.subTest(suite=suite):
                metadata, stored, measure = self.worker_run(suite, sent)
                self.assertFalse(metadata["status"]["success"])
                self.assertEqual(metadata["status"]["error"]["message"], "unexpected recorded durable binary manifest")
                self.assertEqual(stored, [])
                measure.assert_not_called()

    def test_the_runtime_takes_a_seal_for_exactly_the_binaries_of_the_plan(self):
        cloud = ProductionCloud()
        state, _ = cloud.service.load()
        # the stored run is of a suite without tool cells: its seal is the six keys it always was
        self.assertEqual(set(cloud.service.sealed_inputs(state)), set(common.BINARIES))
        entry = state["sealed_inputs"]["candidate/rcp"]
        added = {key: dict(entry, key=entry["key"].replace("candidate/rcp", key))
                 for key in common.suite_binaries(SUITE) if key not in common.BINARIES}
        self.assertEqual(len(added), 6)
        spare = copy.deepcopy(state)
        spare["sealed_inputs"].update(added)
        with self.assertRaisesRegex(ValueError, "production input seal is missing"):
            cloud.service.sealed_inputs(spare)
        # under a plan that names the tools the same six are short of them
        timed = copy.deepcopy(state)
        timed["request"].update(suite=SUITE, plan=common.durable_plan_for_suite(SUITE))
        with self.assertRaisesRegex(ValueError, "production input seal is missing"):
            cloud.service.sealed_inputs(timed)
        # and all twelve are taken, each still held to the admitted manifest and request
        timed["sealed_inputs"].update(added)
        timed["request"]["sha256"].update({key: entry["sha256"] for key in added})
        admitted = cloud.service.record.admission["initial_payload"]["manifest"]["binaries"]
        with self.assertRaises((KeyError, ValueError)):
            cloud.service.sealed_inputs(timed)
        admitted.update({key: dict(sha256=entry["sha256"], bytes=entry["bytes"]) for key in added})
        self.assertEqual(set(cloud.service.sealed_inputs(timed)), set(common.suite_binaries(SUITE)))
        timed["sealed_inputs"]["candidate/rrm"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "sealed binary differs from admitted manifest"):
            cloud.service.sealed_inputs(timed)

    def test_the_comparison_label_covers_every_binary_of_the_plan(self):
        expected, window = dict(baseline="a" * 40, candidate="a" * 40), dict(plan=common.durable_plan_for_suite(SUITE))
        hashes = dict.fromkeys(common.suite_binaries(SUITE), "d" * 64)
        label = lambda sha256, plan=window: results.comparison_label(expected, dict(window=plan,
                                                                                   request=dict(sha256=sha256)))
        self.assertEqual(label(hashes), "A/A (identical source revision and binary digests)")
        self.assertEqual(label(hashes | {"candidate/rrm": "e" * 64}),
                         "Same source revision; different binary digests (not A/A)")
        self.assertEqual(label({key: value for key, value in hashes.items() if not key.endswith("/rrm")}),
                         "Same source revision; binary identity unavailable")
        # a window from before plans names the first three tools, as it always did
        self.assertEqual(label(dict.fromkeys(common.BINARIES, "d" * 64), {}),
                         "A/A (identical source revision and binary digests)")

    def test_the_access_lists_fit_a_role_with_the_longest_bucket_name(self):
        for name, value in stack.TEMPLATE["Resources"].items():
            if value["Type"] != "AWS::IAM::Role":
                continue
            policies = [policy["PolicyDocument"] for policy in stack.resolve(value["Properties"]["Policies"])]
            total = sum(len(json.dumps(policy, separators=(",", ":"))) for policy in policies)
            # a bucket name may be 63 characters; the test stack's is shorter, and is named in every key
            longest = total + json.dumps(policies).count(stack.BUCKET) * (63 - len(stack.BUCKET))
            with self.subTest(role=name):
                self.assertLess(longest, 10_240)


V4, V5 = "rcp-production-public-report-v4", "rcp-production-public-report-v5"
FAILED = dict(status="failed", stage="command", message="run 1 failed with exit code 3; see 0.stdout.log and "
                                                         "0.stderr.log in logs/commission-64m/local-rlink/1")
BOTH = "matrix-commission-local+tools+remote+tools"


class ToolRecordTests(unittest.TestCase):
    """The collector and the public record take tool cells as they take copies, each under its own variant."""

    collect = test_matrix_results.MatrixCollectionTests.collect

    def test_the_published_variants_are_the_ones_the_runner_runs(self):
        # of the tool cells the runner knows, the ones a suite can select
        selectable = {scenario: cell for scenario, cell in matrix.TOOLS.items()
                      if scenario in common.MATRIX_TOOL_CELLS}
        self.assertEqual(projection.PUBLIC_CONTRACTS[V5]["tool_variants"],
                         {scenario: cell["variant"]["id"] for scenario, cell in selectable.items()})
        self.assertEqual({key: value for key, value in projection.PUBLIC_CONTRACTS[V5].items()
                          if key not in ("tool_variants", "validation")},
                         {key: value for key, value in projection.PUBLIC_CONTRACTS[V4].items() if key != "validation"})
        self.assertEqual(diagnostics.TOOL_VARIANTS, projection.PUBLIC_CONTRACTS[V5]["tool_variants"])
        self.assertEqual({scenario: tool for scenario, (tool, _) in diagnostics.TOOL_CELLS.items()},
                         {scenario: cell["tool"] for scenario, cell in selectable.items()})

    def test_a_run_with_tool_cells_publishes_each_cell_under_what_it_ran(self):
        record = matrix_record(SUITE, {2: FAILED})
        result, report, public = self.collect(SUITE, record)
        self.assertEqual((result["state"], result["retired"], report["benchmark_valid"]), ("success", True, True),
                         public["diagnostics"])
        self.assertEqual(public["schema"], V5)
        self.assertEqual([(cell["case_id"], cell["scenario"], cell["outcome"]) for cell in public["cells"]], [
            ("commission-64m", "local", "complete"), ("commission-64m", "local-rcmp", "complete"),
            ("commission-64m", "local-rlink", "failed"), ("commission-64m", "local-rrm", "complete"),
            ("commission-10k", "local", "complete"), ("commission-10k", "local-rcmp", "complete"),
            ("commission-10k", "local-rlink", "complete"), ("commission-10k", "local-rrm", "complete")])
        self.assertEqual((public["cells"][2]["stage"], public["cells"][2]["message"]),
                         ("command", FAILED["message"]))
        self.assertEqual([(item["case_id"], item["scenario"], item["variant_id"]) for item in public["measurements"]], [
            ("commission-64m", "local", "rcp-default"), ("commission-64m", "local-rcmp", "rcmp-size"),
            ("commission-64m", "local-rrm", "rrm-default"),
            ("commission-10k", "local", "rcp-default"), ("commission-10k", "local-rcmp", "rcmp-size"),
            ("commission-10k", "local-rlink", "rlink-default"), ("commission-10k", "local-rrm", "rrm-default")])
        samples = [item["elapsed_seconds"] for item in record["cells"][3]["copies"]]
        self.assertEqual(public["measurements"][2]["samples"], samples)
        # each cell is a series of its own, joined to its own past
        self.assertEqual(len({item["series_id"] for item in public["measurements"]}), 7)
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)), "7 of 8 measurements completed")
        self.assertEqual(public["validation"], "Not verified: a copy or a tool run counts when its command exits "
                                               "cleanly and reports the expected counts.")
        self.assertEqual(public["note"], "Each measurement is the median of 3 copies or tool runs.")

    def test_the_same_tool_after_local_and_after_remote_copies_is_two_cells_of_one_variant(self):
        result, report, public = self.collect(BOTH, matrix_record(BOTH))
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        self.assertEqual(len(public["cells"]), 16)
        both = [item for item in public["measurements"] if item["case_id"] == "commission-10k"
                and item["scenario"] in ("local-rrm", "remote-rrm")]
        self.assertEqual([item["variant_id"] for item in both], ["rrm-default"] * 2)
        self.assertNotEqual(both[0]["series_id"], both[1]["series_id"])

    def test_the_copies_of_a_run_with_tool_cells_stay_on_the_trends_they_had(self):
        with_tools = self.collect(SUITE, matrix_record(SUITE))[2]
        plain = self.collect(PLAIN, matrix_record(PLAIN))[2]
        self.assertEqual(plain["schema"], V4)
        self.assertEqual(plain["note"], "Each measurement is the median of 3 copies.")
        self.assertEqual([item["series_id"] for item in with_tools["measurements"] if item["scenario"] == "local"],
                         [item["series_id"] for item in plain["measurements"]])

    def test_every_cell_of_the_largest_suite_fits_one_record(self):
        record = matrix_record(WEEKLY, {index: dict(status="not_run", stage=None,
                                                    message="allocation ended before this cell")
                                        for index in range(28, 32)})
        result, report, public = self.collect(WEEKLY, record, remaining=42700, runtime_seconds=43200,
                                              capacity_seconds=7200)
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        self.assertEqual(len(public["cells"]), 32)
        order = ["local", *AFTER_LOCAL, "remote", *AFTER_REMOTE]
        self.assertEqual([item["scenario"] for item in public["measurements"]], order * 3 + order[:4])
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)), "28 of 32 measurements completed")

    def test_every_tool_that_was_measured_must_be_an_approved_binary(self):
        for name in TOOLS:
            record = matrix_record(SUITE)
            record["tools"][name]["sha256"] = "e" * 64
            with self.subTest(name=name):
                _, report, public = self.collect(SUITE, record)
                self.assertFalse(report["benchmark_valid"])
                self.assertEqual(public["diagnostics"],
                                 ["qualification: measured tool does not match approved binary digest"])
                self.assertEqual(public["cells"], [])
        # and a run that was sent the tools must have run them
        record = matrix_record(SUITE)
        del record["tools"]["rrm"]
        self.assertFalse(self.collect(SUITE, record)[1]["benchmark_valid"])

    def test_a_run_whose_rrm_stopped_clearing_its_trees_says_so_and_still_counts(self):
        record = matrix_record(SUITE)
        record["rrm_fallback"] = "rrm exited with code 5 while clearing a tree <b>"
        result, report, public = self.collect(SUITE, record)
        self.assertEqual((result["state"], report["benchmark_valid"], len(public["measurements"])),
                         ("success", True, 8))
        # the collector's own words: nothing the worker wrote reaches the page
        self.assertEqual(public["diagnostics"],
                         ["worker: rrm failed while clearing a tree; rm -rf cleared the trees after that"])
        self.assertEqual(self.collect(SUITE, matrix_record(SUITE))[2]["diagnostics"], [])

    def test_a_measurement_names_the_variant_of_its_scenario_and_no_other(self):
        record, _ = matrix_example(BOTH)
        self.assertEqual(record["schema"], V5)
        scenarios = [item["scenario"] for item in record["measurements"]]
        local, remote = scenarios.index("local-rrm"), scenarios.index("remote-rrm")
        for change in (lambda value: value["measurements"][local].update(variant_id="rcp-default"),
                       lambda value: value["measurements"][remote].update(variant_id="rcp-default"),
                       lambda value: value["measurements"][0].update(variant_id="rrm-default"),
                       lambda value: value["measurements"][remote].update(variant_id="rlink-default"),
                       # the earlier version has one variant, and a tool cell is none of its cells' business
                       lambda value: value.update(schema=V4,
                                                  validation=projection.PUBLIC_CONTRACTS[V4]["validation"])):
            broken = json.loads(json.dumps(record))
            change(broken)
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "invalid public"):
                projection.validate_public(broken)
        # a record of the earlier version still reads as it always did
        earlier, _ = matrix_example("matrix-commission-local+remote")
        self.assertEqual(earlier["schema"], V4)
        projection.validate_public(earlier)


def sampled_phase(name, case_index, trial_index=None):
    """One phase as the runner's sampler writes it, with every figure present and large."""
    metrics = {}
    for key in diagnostics.PHASE_METRICS:
        metrics[key] = (dict(status="available", first=10 ** 11, last=10 ** 11, minimum=10 ** 11, maximum=2 * 10 ** 11)
                        if key in diagnostics.GAUGES else
                        dict(status="available", first=10 ** 12, last=3 * 10 ** 12, minimum=10 ** 12,
                             maximum=3 * 10 ** 12, delta=2 * 10 ** 12))
    return dict(phase=name, case_index=case_index, trial_index=trial_index, status="observed",
                elapsed_seconds=612.345678, samples=20, metrics=metrics)


def sampled_matrix(suite, descriptor):
    """A run of a suite as its runner records it when every phase was sampled, and how many phases that is."""
    raw = matrix_record(suite, environment=test_matrix_diagnostics.ENVIRONMENT)
    raw["context"]["system_telemetry"] = dict(descriptor)
    cases = [item["case"]["id"] for item in raw["file_sets"]]
    # what the runner marks: a file set is checked, generated and cleared; a copy has its cache
    # step and its command; rcmp and rrm a command each; rlink its command and the clearing of its tree
    run_phases = [sampled_phase("setup", None)] + [sampled_phase(name, index) for index in range(len(cases))
                                                   for name in ("capacity", "generation", "cleanup")]
    raw["system_telemetry"] = dict(status="complete", samples=20 * len(run_phases), phases=run_phases)
    total = len(run_phases)
    for cell in raw["cells"]:
        names = (("cache", "command") if cell["scenario"] in ("local", "remote") else
                 ("command", "cleanup") if cell["scenario"].endswith("-rlink") else ("command",))
        for item in cell["copies"]:
            phases = [sampled_phase(name, cases.index(cell["case_id"]), item["trial_index"]) for name in names]
            item["system_telemetry"] = dict(status="complete", samples=20 * len(phases), phases=phases)
            total += len(phases)
    matrix.validate(raw)
    return raw, total


class ToolDiagnosticsTests(unittest.TestCase):
    """The resource companion reports a tool run as it reports a copy: by its own process, with no daemons."""

    def test_a_tool_run_has_its_own_figures_its_own_variant_and_no_daemons(self):
        record, extra = matrix_example(BOTH)
        self.assertEqual(extra["schema"], "rcp-public-resource-diagnostics-v4")
        self.assertEqual(extra["status"], "complete")
        self.assertEqual(len(extra["trials"]), 48)
        by_scenario = {}
        for trial in extra["trials"]:
            by_scenario.setdefault(trial["scenario"], []).append(trial)
        self.assertEqual({scenario: {trial["variant_id"] for trial in trials} for scenario, trials in by_scenario.items()},
                         {"local": {"rcp-default"}, "remote": {"rcp-default"},
                          "local-rcmp": {"rcmp-size"}, "local-rlink": {"rlink-default"}, "local-rrm": {"rrm-default"},
                          "remote-rcmp": {"rcmp-size"}, "remote-rlink": {"rlink-default"},
                          "remote-rrm": {"rrm-default"}})
        for scenario, trials in by_scenario.items():
            for trial in trials:
                self.assertEqual(trial["metrics"]["max_rss_kib"], 90000)
                # only a remote copy starts daemons: a tool that follows one is a process of its own
                self.assertEqual(trial["roles"] is not None, scenario == "remote", scenario)
        self.assertEqual(extra["process_scope"], diagnostics.TOOLS_PROCESS_SCOPE)
        self.assertIn("A tool run is one process", extra["process_scope"])
        diagnostics.validate(extra, record)

    def test_without_remote_cells_no_trial_has_daemons_and_the_document_is_complete(self):
        record, extra = matrix_example(SUITE)
        self.assertEqual((extra["schema"], extra["status"]), ("rcp-public-resource-diagnostics-v4", "complete"))
        self.assertEqual({trial["roles"] for trial in extra["trials"]}, {None})

    def test_the_companion_of_a_tool_run_is_checked_against_its_record(self):
        record, extra = matrix_example(BOTH)
        tool = next(index for index, trial in enumerate(extra["trials"]) if trial["scenario"] == "remote-rrm")
        copy = next(index for index, trial in enumerate(extra["trials"]) if trial["scenario"] == "local")
        for change in (lambda value: value["trials"][tool].update(variant_id="rcp-default"),
                       lambda value: value["trials"][copy].update(variant_id="rrm-default"),
                       lambda value: value["trials"][tool].update(roles=dict(source=None, destination=None)),
                       lambda value: value.update(schema="rcp-public-resource-diagnostics-v3",
                                                  process_scope=diagnostics.REMOTE_PROCESS_SCOPE)):
            broken = json.loads(json.dumps(extra))
            change(broken)
            with self.subTest(change=change), self.assertRaises(ValueError):
                diagnostics.validate(broken, record)

    def test_earlier_companions_keep_their_version_and_their_rules(self):
        for suite, schema in (("matrix-commission-local", "rcp-public-resource-diagnostics-v2"),
                              ("matrix-commission-local+remote", "rcp-public-resource-diagnostics-v3")):
            record, extra = matrix_example(suite)
            with self.subTest(suite=suite):
                self.assertEqual(extra["schema"], schema)
                self.assertEqual({trial["variant_id"] for trial in extra["trials"]}, {"rcp-default"})

    def test_a_tool_failure_leaves_the_other_trials_published(self):
        record, extra = matrix_example(SUITE, outcomes={2: FAILED})
        failed = [trial for trial in extra["trials"]
                  if (trial["case_id"], trial["scenario"]) == ("commission-64m", "local-rlink")]
        self.assertEqual([(trial["status"], trial["metrics"]) for trial in failed], [("unavailable", None)])
        self.assertEqual(extra["status"], "partial")
        self.assertEqual(len(extra["trials"]), 22)

    def test_the_run_page_names_the_process_of_each_tool_run(self):
        record, extra = matrix_example(BOTH)
        html = diagnostics.render(extra, record)
        self.assertIn("<th>File set</th><th>Scenario</th><th>Copy or run</th><th>Process</th>", html)
        for scenario, process in (("local", "rcp"), ("local-rcmp", "rcmp"), ("local-rlink", "rlink"),
                                  ("local-rrm", "rrm"), ("remote", "rcp"), ("remote", "source rcpd"),
                                  ("remote-rcmp", "rcmp"), ("remote-rrm", "rrm")):
            self.assertIn("<td><code>commission-64m</code></td><td><code>" + scenario + "</code></td><td>1</td><td>" +
                          process + "</td>", html)
        self.assertNotIn("<td><code>remote-rrm</code></td><td>1</td><td>source rcpd</td>", html)

    def test_the_sampled_detail_of_the_largest_suite_fits_the_public_document(self):
        raw, total = sampled_matrix(WEEKLY, diagnostics.TELEMETRY_DESCRIPTOR)
        # 24 copies of 6 windows each, and 13 of the run: well inside the sampler's limit of 192
        self.assertEqual(total, 157)
        self.assertLess(total, diagnostics.TELEMETRY_DESCRIPTOR["max_phases"] - 24)
        record, _ = matrix_example(WEEKLY)
        extra = diagnostics.derive(record, b"archive", raw, test_matrix_diagnostics.WORKER)
        # nothing had to be left out to stay within the public document's size
        self.assertEqual((extra["status"], extra["reason"]), ("complete", None))
        self.assertEqual(len(extra["trials"]), 96)
        self.assertEqual(sum(len(trial["system"]["phases"]) for trial in extra["trials"]) +
                         len(extra["system"]["phases"]), 157)
        size = len(json.dumps(extra, separators=(",", ":")))
        self.assertLess(size, diagnostics.MAX_DOCUMENT * 3 // 4, size)


if __name__ == "__main__":
    unittest.main()
