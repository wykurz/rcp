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
from . import results, test_matrix_results, test_production_stack as stack, worker
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
WITH_TOOLS = ("local+tools", "local+tools+remote")


def properties(argv):
    return dict(item[len("--property="):].split("=", 1) for item in argv if item.startswith("--property="))


class ToolSuiteTests(unittest.TestCase):
    def test_the_tools_are_the_ones_the_runner_times_in_the_order_it_runs_them(self):
        self.assertEqual(common.MATRIX_TOOLS, tuple(matrix.TOOLS))
        self.assertEqual(common.MATRIX_TOOLS, TOOLS)
        # each tool is its own program, named as its scenario
        self.assertEqual([matrix.TOOLS[name]["variant"]["tool"] for name in TOOLS], list(TOOLS))

    def test_every_selection_has_a_suite_with_the_tools_and_one_with_remote_copies_as_well(self):
        self.assertEqual(sorted(common.MATRIX_SUITES), sorted(
            f"matrix-{selection}-{scenarios}" for selection in SELECTIONS for scenarios in (*EARLIER, *WITH_TOOLS)))
        # the tools of a file set come right after its local cell, whose copies they work on
        self.assertEqual(common.MATRIX_SUITES["matrix-1k-local+tools"], tuple(
            ("tiny-1k-10240k", scenario) for scenario in ("local", *TOOLS)))
        self.assertEqual(common.MATRIX_SUITES["matrix-all-local+tools+remote"], tuple(
            (case, scenario) for case in FILE_SETS for scenario in ("local", *TOOLS, "remote")))
        self.assertEqual(common.MATRIX_SUITES["matrix-commission-local+tools"], tuple(
            (case, scenario) for case in ("commission-64m", "commission-10k") for scenario in ("local", *TOOLS)))
        self.assertEqual(common.SUITES["matrix-all-local+tools+remote"], FILE_SETS)
        for suite, cells in common.MATRIX_SUITES.items():
            with self.subTest(suite=suite):
                # a suite name says whether it times the tools
                self.assertEqual(any(scenario in TOOLS for _, scenario in cells), "+tools" in suite)
                self.assertLessEqual({scenario for _, scenario in cells}, set(matrix.SCENARIOS))
                # and the runner accepts its selection: every tool cell has its local cell
                manifest = matrix.run.load_manifest(ROOT / "benchmarks/matrix-cases.json")
                selected = matrix.selection(manifest, [case + ":" + scenario for case, scenario in cells])
                self.assertEqual([(case["id"], scenario) for case, scenario in selected], list(cells))

    def test_the_suites_of_earlier_releases_keep_their_place_and_their_plans(self):
        earlier = [f"matrix-{selection}-{scenarios}" for scenarios in EARLIER for selection in SELECTIONS]
        self.assertEqual(common.DURABLE_SUITES[5:23], tuple(earlier))
        self.assertEqual(common.DURABLE_SUITES[23:], tuple(
            f"matrix-{selection}-{scenarios}" for scenarios in WITH_TOOLS for selection in SELECTIONS))
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

    def test_time_budgets_are_those_of_the_same_selection_without_the_tools(self):
        for selection in SELECTIONS:
            plain = common.durable_plan_contract(common.durable_plan_for_suite(f"matrix-{selection}-local+remote"))
            for scenarios in WITH_TOOLS:
                suite = f"matrix-{selection}-{scenarios}"
                with self.subTest(suite=suite):
                    contract = common.durable_plan_contract(common.durable_plan_for_suite(suite))
                    skipped = ("suite", "plan", "binary_names", "binaries")
                    self.assertEqual({key: contract[key] for key in contract if key not in skipped},
                                     {key: plain[key] for key in plain if key not in skipped})
                    self.assertEqual(contract["plan"], dict(plain["plan"], version=suite + "-plan-v1"))
                    self.assertEqual(common.matrix_contract(suite), dict(
                        common.matrix_contract(f"matrix-{selection}-local+remote"), cells=common.MATRIX_SUITES[suite]))
        versions = [common.durable_plan_for_suite(suite)["version"] for suite in common.DURABLE_SUITES]
        self.assertEqual(len(set(versions)), len(versions))

    def test_the_worker_passes_the_tool_cells_to_the_runner(self):
        argv = worker.command(dict(suite="matrix-commission-local+tools+remote", baseline="a" * 40), 9000)
        args = matrix._arguments(argv[argv.index("benchmarks.matrix") + 1:])
        self.assertEqual(args.cells, [
            "commission-64m:local", "commission-64m:rcmp", "commission-64m:rlink", "commission-64m:rrm",
            "commission-64m:remote",
            "commission-10k:local", "commission-10k:rcmp", "commission-10k:rlink", "commission-10k:rrm",
            "commission-10k:remote"])
        self.assertEqual(args.bin_dir, worker.STATE / "candidate")
        # the tools open no socket: only a suite with remote copies may
        with_remote = properties(argv)
        without = properties(worker.command(dict(suite="matrix-commission-local+tools", baseline="a" * 40), 9000))
        self.assertEqual((with_remote["RestrictAddressFamilies"], without["RestrictAddressFamilies"]),
                         ("AF_UNIX AF_INET", "AF_UNIX"))
        plain = properties(worker.command(dict(suite="matrix-commission-local", baseline="a" * 40), 9000))
        self.assertEqual(without, plain)


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
        self.assertEqual([item for item in argv if item.endswith(":rrm")], ["commission-64m:rrm", "commission-10k:rrm"])
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
FAILED = dict(status="failed", stage="command",
              message="run 1 failed with exit code 3; see 0.stdout.log and 0.stderr.log in logs/commission-64m/rlink/1")


class ToolRecordTests(unittest.TestCase):
    """The collector and the public record take tool cells as they take copies, each under its own variant."""

    collect = test_matrix_results.MatrixCollectionTests.collect

    def test_the_published_variants_are_the_ones_the_runner_runs(self):
        self.assertEqual(projection.PUBLIC_CONTRACTS[V5]["tool_variants"],
                         {name: tool["variant"]["id"] for name, tool in matrix.TOOLS.items()})
        self.assertEqual({key: value for key, value in projection.PUBLIC_CONTRACTS[V5].items()
                          if key not in ("tool_variants", "validation")},
                         {key: value for key, value in projection.PUBLIC_CONTRACTS[V4].items() if key != "validation"})

    def test_a_run_with_tool_cells_publishes_each_cell_under_what_it_ran(self):
        record = matrix_record(SUITE, {2: FAILED})
        result, report, public = self.collect(SUITE, record)
        self.assertEqual((result["state"], result["retired"], report["benchmark_valid"]), ("success", True, True),
                         public["diagnostics"])
        self.assertEqual(public["schema"], V5)
        self.assertEqual([(cell["case_id"], cell["scenario"], cell["outcome"]) for cell in public["cells"]], [
            ("commission-64m", "local", "complete"), ("commission-64m", "rcmp", "complete"),
            ("commission-64m", "rlink", "failed"), ("commission-64m", "rrm", "complete"),
            ("commission-10k", "local", "complete"), ("commission-10k", "rcmp", "complete"),
            ("commission-10k", "rlink", "complete"), ("commission-10k", "rrm", "complete")])
        self.assertEqual((public["cells"][2]["stage"], public["cells"][2]["message"]),
                         ("command", FAILED["message"]))
        self.assertEqual([(item["case_id"], item["scenario"], item["variant_id"]) for item in public["measurements"]], [
            ("commission-64m", "local", "rcp-default"), ("commission-64m", "rcmp", "rcmp-size"),
            ("commission-64m", "rrm", "rrm-default"),
            ("commission-10k", "local", "rcp-default"), ("commission-10k", "rcmp", "rcmp-size"),
            ("commission-10k", "rlink", "rlink-default"), ("commission-10k", "rrm", "rrm-default")])
        samples = [item["elapsed_seconds"] for item in record["cells"][3]["copies"]]
        self.assertEqual(public["measurements"][2]["samples"], samples)
        # each cell is a series of its own, joined to its own past
        self.assertEqual(len({item["series_id"] for item in public["measurements"]}), 7)
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)), "7 of 8 measurements completed")
        self.assertEqual(public["validation"], "Not verified: a copy or a tool run counts when its command exits "
                                               "cleanly and reports the expected counts.")
        self.assertEqual(public["note"], "Each measurement is the median of 3 copies or tool runs.")

    def test_the_copies_of_a_run_with_tool_cells_stay_on_the_trends_they_had(self):
        with_tools = self.collect(SUITE, matrix_record(SUITE))[2]
        plain = self.collect(PLAIN, matrix_record(PLAIN))[2]
        self.assertEqual(plain["schema"], V4)
        self.assertEqual(plain["note"], "Each measurement is the median of 3 copies.")
        self.assertEqual([item["series_id"] for item in with_tools["measurements"] if item["scenario"] == "local"],
                         [item["series_id"] for item in plain["measurements"]])

    def test_every_cell_of_the_weekly_suite_with_tools_fits_one_record(self):
        suite = "matrix-all-local+tools+remote"
        record = matrix_record(suite, {19: dict(status="not_run", stage=None,
                                                message="allocation ended before this cell")})
        result, report, public = self.collect(suite, record, remaining=28300, runtime_seconds=28800,
                                              capacity_seconds=7200)
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        self.assertEqual(len(public["cells"]), 20)
        self.assertEqual([item["scenario"] for item in public["measurements"]],
                         ["local", "rcmp", "rlink", "rrm", "remote"] * 3 + ["local", "rcmp", "rlink", "rrm"])

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

    def test_a_measurement_names_the_variant_of_its_scenario_and_no_other(self):
        record, _ = matrix_example(SUITE)
        self.assertEqual(record["schema"], V5)
        index = [item["scenario"] for item in record["measurements"]].index("rrm")
        for change in (lambda value: value["measurements"][index].update(variant_id="rcp-default"),
                       lambda value: value["measurements"][0].update(variant_id="rrm-default"),
                       lambda value: value["measurements"][index].update(variant_id="rlink-default"),
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


class ToolDiagnosticsTests(unittest.TestCase):
    """The resource companion reports a tool run as it reports a copy: by its own process, with no daemons."""

    def test_a_tool_run_has_its_own_figures_its_own_variant_and_no_daemons(self):
        record, extra = matrix_example("matrix-commission-local+tools+remote")
        self.assertEqual(extra["schema"], "rcp-public-resource-diagnostics-v4")
        self.assertEqual(extra["status"], "complete")
        self.assertEqual(len(extra["trials"]), 30)
        by_scenario = {}
        for trial in extra["trials"]:
            by_scenario.setdefault(trial["scenario"], []).append(trial)
        self.assertEqual({scenario: {trial["variant_id"] for trial in trials} for scenario, trials in by_scenario.items()},
                         dict(local={"rcp-default"}, rcmp={"rcmp-size"}, rlink={"rlink-default"}, rrm={"rrm-default"},
                              remote={"rcp-default"}))
        for scenario, trials in by_scenario.items():
            for trial in trials:
                self.assertEqual(trial["metrics"]["max_rss_kib"], 90000)
                # only a remote copy starts daemons
                self.assertEqual(trial["roles"] is not None, scenario == "remote", scenario)
        self.assertEqual(extra["process_scope"], diagnostics.TOOLS_PROCESS_SCOPE)
        self.assertIn("A tool run is one process", extra["process_scope"])
        diagnostics.validate(extra, record)

    def test_without_remote_cells_no_trial_has_daemons_and_the_document_is_complete(self):
        record, extra = matrix_example(SUITE)
        self.assertEqual((extra["schema"], extra["status"]), ("rcp-public-resource-diagnostics-v4", "complete"))
        self.assertEqual({trial["roles"] for trial in extra["trials"]}, {None})

    def test_the_companion_of_a_tool_run_is_checked_against_its_record(self):
        record, extra = matrix_example("matrix-commission-local+tools+remote")
        tool = next(index for index, trial in enumerate(extra["trials"]) if trial["scenario"] == "rrm")
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
        failed = [trial for trial in extra["trials"] if (trial["case_id"], trial["scenario"]) == ("commission-64m", "rlink")]
        self.assertEqual([(trial["status"], trial["metrics"]) for trial in failed], [("unavailable", None)])
        self.assertEqual(extra["status"], "partial")
        self.assertEqual(len(extra["trials"]), 22)

    def test_the_run_page_names_the_process_of_each_tool_run(self):
        record, extra = matrix_example("matrix-commission-local+tools+remote")
        html = diagnostics.render(extra, record)
        self.assertIn("<th>File set</th><th>Scenario</th><th>Copy or run</th><th>Process</th>", html)
        for scenario, process in (("local", "rcp"), ("rcmp", "rcmp"), ("rlink", "rlink"), ("rrm", "rrm"),
                                  ("remote", "rcp"), ("remote", "source rcpd")):
            self.assertIn("<td><code>commission-64m</code></td><td><code>" + scenario + "</code></td><td>1</td><td>" +
                          process + "</td>", html)
        self.assertNotIn("<td><code>rrm</code></td><td>1</td><td>source rcpd</td>", html)


if __name__ == "__main__":
    unittest.main()
