"""Offline file-set qualification boundaries; no cloud calls or large fixture creation."""

import copy
from dataclasses import asdict
from datetime import timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

from . import common, durable, production_client, production_handler, production_policy, worker
from .test_durable_contracts import NOW as REQUEST_TIME, bound_window, request as one_gigabyte_request
from .test_durable_worker import direct_window, durable_window, request as worker_request
from .test_production_admission import Cloud
from .test_production_handler import policy_item
from .test_production_policy import NOW, identity, inputs, policy, run_json
from .test_production_workflow_request import environment
from .test_results import binding, measurement
from .test_worker_review import frozen_worker_time, report, worker_fixture

ROOT = Path(__file__).resolve().parents[2]
SUITE = "bulk-100m-metadata"
OLD_SUITE = "bulk-1g-metadata"
PLAN = dict(version="bulk-100m-qualification-budget-v1", minimum_worker_seconds=10200,
            boot_reserve_seconds=300, shutdown_reserve_seconds=300)
OLD_PLAN = dict(version="bulk-metadata-plan-v1", work_seconds=3104, setup_seconds=100,
                report_seconds=180, safety_factor=3, boot_reserve_seconds=300,
                shutdown_reserve_seconds=300)


def qualification_request():
    original = one_gigabyte_request()
    return durable.request(dict(run=original["run"], baseline=original["baseline"],
        candidate=original["candidate"], suite=SUITE, controller=original["controller"]),
        REQUEST_TIME, original["sha256"], original["pins"])


def authorize(configured, suite, who=None):
    who = who or identity()
    observed = production_policy.WorkflowRun.verify(run_json(who), who, NOW)
    return production_policy.authorize(configured, who, observed, inputs(suite=suite), NOW)


class FileSetPlanTests(unittest.TestCase):
    def test_old_plan_bytes_and_existing_headroom_are_unchanged(self):
        expected = b'{"boot_reserve_seconds":300,"report_seconds":180,"safety_factor":3,"setup_seconds":100,"shutdown_reserve_seconds":300,"version":"bulk-metadata-plan-v1","work_seconds":3104}'
        original = one_gigabyte_request()
        self.assertEqual(production_policy.canonical(original["plan"]), expected)
        self.assertEqual(original["plan"], OLD_PLAN)
        self.assertEqual(common.durable_worker_reserve(original["plan"]), 9852)
        self.assertEqual(durable.minimum_runtime(original["plan"]), 10452)
        self.assertEqual(original["runtime_seconds"], 10800)
        self.assertEqual(common.durable_plan_for_suite(OLD_SUITE), OLD_PLAN)

    def test_new_plan_is_a_fixed_budget_without_claiming_a_duration_estimate(self):
        value = qualification_request()
        contract = common.durable_plan_contract(value["plan"])
        self.assertEqual(value["plan"], PLAN)
        self.assertNotIn("work_seconds", value["plan"])
        self.assertNotIn("safety_factor", value["plan"])
        self.assertEqual(common.durable_worker_reserve(value["plan"]), 10200)
        self.assertEqual(durable.minimum_runtime(value["plan"]), 10800)
        self.assertEqual(contract["suite"], SUITE)
        self.assertIn("not yet measured", contract["timing_basis"])
        self.assertEqual((contract["max_runtime"], contract["max_provisioning"],
                          contract["delivery_drain_seconds"]), (10800, 43200, 900))
        with self.assertRaises(TypeError):
            common._RECORDED_DURABLE_CONTRACTS[PLAN["version"]]["plan"]["minimum_worker_seconds"] = 1
        changed = common.durable_plan_for_suite(SUITE)
        changed["minimum_worker_seconds"] = 1
        self.assertEqual(common.durable_plan_for_suite(SUITE), PLAN)

    def test_requests_and_windows_reject_cross_file_set_plan_rebinding(self):
        for value, foreign in ((one_gigabyte_request(), PLAN), (qualification_request(), OLD_PLAN)):
            window = bound_window(value)
            value["plan"] = dict(foreign)
            window["plan"] = dict(foreign)
            for exact in (True, False):
                with self.subTest(suite=value["suite"], exact=exact):
                    with self.assertRaisesRegex(ValueError, "another file set"):
                        durable.validate(value, exact=exact)
                    with self.assertRaisesRegex(ValueError, "another file set"):
                        common.validate_durable_window(window, REQUEST_TIME + timedelta(seconds=90), 0, exact=exact)

    def test_qualification_runtime_rejects_one_second_below_or_above_existing_ceiling(self):
        for seconds in (10799, 10800, 10801):
            value = qualification_request()
            window = bound_window(value)
            value["runtime_seconds"] = seconds
            value["outer_deadline"] = common.timestamp(common.utc(value["capacity_deadline"]) + timedelta(seconds=seconds))
            window.update(runtime_seconds=seconds, outer_deadline=value["outer_deadline"],
                          deadline=common.timestamp(REQUEST_TIME + timedelta(seconds=seconds - 300)))
            checks = (lambda: durable.validate(value),
                      lambda: common.validate_durable_window(window, REQUEST_TIME + timedelta(seconds=90), 0))
            for check in checks:
                if seconds == 10800:
                    check()
                else:
                    with self.subTest(seconds=seconds), self.assertRaisesRegex(ValueError, "runtime does not fit"):
                        check()

    def test_qualification_cannot_enter_a_legacy_operator_or_asg_window(self):
        legacy = durable_window()
        legacy.update(suite=SUITE, plan=dict(PLAN))
        with self.assertRaisesRegex(ValueError, "direct instance-bound"):
            common.validate_durable_window(legacy, common.utc(legacy["launch_at"]), 0)
        value = dict(run="123-1", baseline="a" * 40, candidate="a" * 40, suite=SUITE,
                     opens=common.timestamp(REQUEST_TIME), deadline=common.timestamp(REQUEST_TIME + timedelta(hours=1)))
        for is_window in (False, True):
            with self.subTest(window=is_window), self.assertRaisesRegex(ValueError, "disabled"):
                common.validate_admission(value, window=is_window)
        common.validate_admission(bound_window(qualification_request()), window=True)

    def test_new_cloud_profile_does_not_invent_old_partial_verification_identities(self):
        latest = dict(policy="metadata-size-tree-v1", workers=8, inline_bytes=1048576,
                      read_bytes=1048576, batch_depth=2)
        self.assertEqual(common.verification_contracts(SUITE), (latest,))
        self.assertEqual(len(common.verification_contracts(OLD_SUITE)), 3)
        for legacy in ("bulk-1g", "bulk-100m", "bulk-1m", "tiny-1k"):
            with self.subTest(suite=legacy):
                self.assertEqual(len(common.verification_contracts(legacy)), 3)
                self.assertEqual(common.verification_contracts(legacy)[0],
                                 dict(policy="sha256-tree-stream-v1", workers=8))
                self.assertNotIn(legacy, common.ACTIVE_SUITES)


class FileSetAdmissionTests(unittest.TestCase):
    def test_legacy_wire_policy_remains_byte_identical_and_one_gigabyte_only(self):
        value = asdict(policy())
        value.pop("manual_suites")
        value.pop("manual_runtime_seconds")
        self.assertEqual(len(value), 17)
        encoded = production_policy.canonical(value).decode()
        item = dict(pk={"S": "policy"}, version={"S": value["version"]},
                    enabled={"BOOL": value["enabled"]}, payload={"S": encoded})
        original = copy.deepcopy(item)
        api = Mock()
        api.get_item.return_value = {"Item": item}
        configured = production_handler.read_policy(api, "production-table", "483465")
        self.assertEqual(item, original)
        self.assertEqual(item["payload"]["S"], encoded)
        self.assertEqual(configured.manual_suites, (OLD_SUITE,))
        authorize(configured, OLD_SUITE)
        with self.assertRaisesRegex(ValueError, "explicitly approved"):
            authorize(configured, SUITE)
        self.assertEqual([call[0] for call in api.mock_calls], ["get_item"])

    def test_manual_qualification_requires_explicit_server_opt_in_without_extra_reservation(self):
        with self.assertRaisesRegex(ValueError, "explicitly approved"):
            authorize(policy(), SUITE)
        configured = policy(manual_suites=(OLD_SUITE, SUITE))
        one, hundred = authorize(configured, OLD_SUITE), authorize(configured, SUITE)
        self.assertEqual(one["reserve_microusd"], hundred["reserve_microusd"])
        self.assertNotEqual(one["fingerprint"], hundred["fingerprint"])
        self.assertEqual(hundred["manifest"]["suite"], SUITE)
        self.assertEqual(hundred["producer"], one["producer"])

    def test_denied_or_unknown_suite_never_reserves_or_issues_upload_capabilities(self):
        for suite in (SUITE, "bulk-1m-metadata", "tiny-1k", "not-a-suite"):
            cloud = Cloud()
            original = copy.deepcopy(cloud.ddb.items)
            with self.subTest(suite=suite), self.assertRaises(ValueError):
                cloud.service.begin("test-only-token", inputs(suite=suite))
            self.assertEqual(cloud.ddb.items, original)
            cloud.api["s3"].generate_presigned_url.assert_not_called()
            cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_opted_in_admission_retains_the_suite_bound_plan_and_original_budget_controls(self):
        cloud = Cloud()
        cloud.policy = policy(manual_suites=(OLD_SUITE, SUITE))
        first = cloud.service.begin("test-only-token", inputs(suite=SUITE))
        record = cloud.service.store(cloud.who).read()
        self.assertEqual(first["state"], "awaiting-inputs")
        self.assertEqual(record.state["request"]["suite"], SUITE)
        self.assertEqual(record.state["request"]["plan"], PLAN)
        self.assertEqual(record.state["request"]["runtime_seconds"], 10800)
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "80052800")
        cloud.service.begin("test-only-token", inputs(suite=SUITE))
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "80052800")
        self.assertEqual(cloud.ddb.items["rate#day#2026-10-22"]["runs"]["N"], "1")
        cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_manual_opt_in_never_grants_a_new_scheduled_suite(self):
        configured = policy(manual_suites=(OLD_SUITE, SUITE), schedule_slots=((3, 19, 30),),
                            scheduled_baseline=production_policy.SCHEDULE_MAIN_AA)
        who = identity(event_name="schedule")
        authorize(configured, OLD_SUITE, who)
        with self.assertRaisesRegex(ValueError, "scheduled file set"):
            authorize(configured, SUITE, who)

    def test_unrecognized_duplicate_and_underfunded_manual_suite_policies_fail_closed(self):
        for suites in ((SUITE, SUITE), ("unrecognized-metadata",), [SUITE], (True,), ([SUITE],)):
            with self.subTest(suites=suites), self.assertRaises(ValueError):
                policy(manual_suites=suites)
        with self.assertRaisesRegex(ValueError, "qualification budget"):
            policy(manual_suites=(SUITE,), runtime_seconds=10799)
        for suites in (None, SUITE, [True], [SUITE, SUITE], ["unknown"]):
            raw = asdict(policy())
            raw["manual_suites"] = suites
            item = policy_item()
            item["payload"]["S"] = production_policy.canonical(raw).decode()
            api = Mock()
            api.get_item.return_value = {"Item": item}
            with self.subTest(wire_suites=suites), self.assertRaisesRegex(ValueError, "policy unavailable"):
                production_handler.read_policy(api, "production-table", "483465")


class FileSetWorkflowTests(unittest.TestCase):
    def test_actual_cli_pins_schedules_and_validates_manual_suite_before_any_output(self):
        cases = [("workflow_dispatch", "", True, OLD_SUITE),
                 ("workflow_dispatch", SUITE, True, SUITE),
                 ("workflow_dispatch", "bulk-1m-metadata", True, "bulk-1m-metadata"),
                 ("workflow_dispatch", "tiny-1k-metadata", True, "tiny-1k-metadata"),
                 ("workflow_dispatch", "all-filesets-metadata", True, "all-filesets-metadata"),
                 ("workflow_dispatch", "unrecognized-metadata", False, None),
                 ("workflow_dispatch", "bulk-100m-metadata\nsuite=untrusted", False, None),
                 ("schedule", "", True, OLD_SUITE),
                 ("schedule", OLD_SUITE, True, OLD_SUITE),
                 ("schedule", SUITE, False, None),
                 ("schedule", "bulk-1m-metadata", False, None),
                 ("schedule", "tiny-1k-metadata", False, None),
                 ("schedule", "all-filesets-metadata", False, None)]
        for event, requested, accepted, expected in cases:
            with self.subTest(event=event, requested=requested), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "output"
                env = dict(os.environ, **environment(GITHUB_EVENT_NAME=event, RCP_SCHEDULE_ENABLED="true"),
                           RCP_REQUESTED_SUITE=requested, GITHUB_OUTPUT=str(output))
                completed = subprocess.run([sys.executable, "-B", "-m", "infra.aws_benchmark.production_workflow"],
                                           cwd=ROOT, env=env, capture_output=True, timeout=5)
                self.assertEqual(completed.returncode == 0, accepted, completed.stderr)
                self.assertEqual(completed.stdout, b"")
                if accepted:
                    self.assertEqual(output.read_text(), "revision=" + env["GITHUB_SHA"] + "\nsuite=" + expected + "\n")
                else:
                    self.assertFalse(output.exists())

    def test_validated_suite_reaches_submission_without_exposing_oidc_to_selected_source(self):
        workflow = yaml.load((ROOT / ".github/workflows/aws-benchmark-production.yml").read_text(), Loader=yaml.BaseLoader)
        selected = workflow["on"]["workflow_dispatch"]["inputs"]["suite"]
        self.assertEqual(selected["default"], OLD_SUITE)
        self.assertEqual(selected["options"], list(common.DURABLE_SUITES))
        self.assertEqual(workflow["jobs"]["validate"]["outputs"]["suite"], "${{ steps.source.outputs.suite }}")
        step = next(x for x in workflow["jobs"]["validate"]["steps"] if x.get("id") == "source")
        self.assertEqual(step["env"]["RCP_REQUESTED_SUITE"], "${{ inputs.suite }}")
        submit = next(x for x in workflow["jobs"]["submit"]["steps"] if x.get("name") == "Reserve, upload data and submit the existing run")
        self.assertEqual(submit["env"]["SUITE"], "${{ needs.validate.outputs.suite }}")
        self.assertIn('--suite "$SUITE"', submit["run"])
        self.assertEqual(workflow["jobs"]["build"]["permissions"], {"contents": "read"})
        self.assertEqual(workflow["jobs"]["build"]["cache-mode"], "none")
        self.assertEqual(workflow["on"]["schedule"], [{"cron": "17 8 * * 1,5"}])

    def test_client_selects_manual_suite_without_changing_the_identical_binary_pair(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("rcp", "rcpd", "filegen"):
                (root / name).write_bytes(("test-only-" + name).encode())
            old, old_binaries = production_client.prepare(root, "a" * 40)
            new, new_binaries = production_client.prepare(root, "a" * 40, suite=SUITE)
            self.assertEqual(old["suite"], OLD_SUITE)
            self.assertEqual(new["suite"], SUITE)
            self.assertEqual(old_binaries, new_binaries)
            self.assertEqual(old["binaries"], new["binaries"])
            for suite in ("bulk-1m-metadata", "tiny-1k-metadata", "all-filesets-metadata"):
                with self.subTest(suite=suite):
                    selected, binaries = production_client.prepare(root, "a" * 40, suite=suite)
                    self.assertEqual(selected["suite"], suite)
                    self.assertEqual(binaries, old_binaries)
                    self.assertEqual(selected["binaries"], old["binaries"])
        with patch.object(production_client, "read_binary") as read:
            with self.assertRaises(ValueError):
                production_client.prepare("unused", "a" * 40, suite="unrecognized-metadata")
            read.assert_not_called()


class FileSetWorkerTests(unittest.TestCase):
    def test_100mb_worker_floor_is_inclusive_and_late_start_never_prepares_storage(self):
        for remaining in (10200, 10199):
            with self.subTest(remaining=remaining), worker_fixture(SUITE) as (root, cloud, measure, stop):
                lease = direct_window()
                lease.update(suite=SUITE, plan=dict(PLAN))
                cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
                cloud.objects[common.REQUEST_KEY] = json.dumps(worker_request(lease)).encode()
                (root / "benchmarks").mkdir()
                (root / "benchmarks/large-cases.json").write_bytes((ROOT / "benchmarks/large-cases.json").read_bytes())
                with frozen_worker_time(cloud) as clock, patch.object(worker, "DURABLE_STATE", root / "persistent"), \
                     patch.object(worker, "instance_identity", return_value=lease["instance_id"]), \
                     patch.object(worker.large_storage, "prepare", return_value={}) as prepare:
                    clock["wall"] = common.utc(lease["deadline"]).timestamp() - remaining
                    worker.run(cloud.api["s3"], "bucket", direct=True)
                metadata, _ = report(cloud)
                self.assertEqual(metadata["admission"]["remaining_seconds"], remaining)
                self.assertEqual(metadata["admission"]["required_seconds"], 10200)
                self.assertEqual(metadata["admission"]["accepted"], remaining == 10200)
                self.assertEqual(prepare.call_count, int(remaining == 10200))
                self.assertEqual(measure.call_count, int(remaining == 10200))
                self.assertEqual(metadata["status"]["success"], remaining == 10200)
                if remaining == 10199:
                    self.assertFalse((root / "state").exists())
                    self.assertEqual([call.kwargs["Key"] for call in cloud.api["s3"].get_object.call_args_list],
                                     [common.WINDOW_KEY, common.REQUEST_KEY])

    def test_worker_uses_budget_label_four_copy_contract_and_100mb_case_without_real_generation(self):
        with worker_fixture(SUITE) as (root, cloud, measure, stop):
            lease = direct_window()
            lease.update(suite=SUITE, plan=dict(PLAN))
            cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
            cloud.objects[common.REQUEST_KEY] = json.dumps(worker_request(lease)).encode()
            (root / "benchmarks").mkdir()
            (root / "benchmarks/large-cases.json").write_bytes((ROOT / "benchmarks/large-cases.json").read_bytes())
            seen = []
            def dispatch(argv, **kwargs):
                from benchmarks import run as harness
                args = harness._arguments(argv[argv.index("benchmarks.run") + 1:])
                self.assertEqual(args.cases, ["bulk-100m-1024gb"])
                self.assertEqual(args.repetitions, 2)
                self.assertEqual(args.paired_seed, 0)
                self.assertEqual(args.verification_mode, "metadata-size")
                self.assertEqual(args.purpose, "performance")
                seen.append(args)
                args.output.mkdir()
                (args.output / "summary.md").write_text("offline fake harness; no large fixtures\n")
                return type("Completed", (), {"returncode": 0})()
            measure.side_effect = dispatch
            with frozen_worker_time(cloud) as clock, patch.object(worker, "DURABLE_STATE", root / "persistent"), \
                 patch.object(worker, "instance_identity", return_value=lease["instance_id"]), \
                 patch.object(worker.large_storage, "prepare", return_value={}) as prepare:
                clock["wall"] = common.utc(lease["attempt_started_at"]).timestamp() + 90
                worker.run(cloud.api["s3"], "bucket", direct=True)
                selected = prepare.call_args.args[1]
                self.assertEqual(selected["id"], "bulk-100m-1024gb")
                self.assertEqual((selected["files_per_leaf"], selected["file_size_bytes"]), (1024, 100000000))
            self.assertEqual(len(seen), 1)
            stop.assert_called_once()
            metadata, _ = report(cloud)
            self.assertTrue(metadata["status"]["success"], metadata["status"])
            self.assertEqual(metadata["admission"]["required_seconds"], 10200)
            self.assertEqual(metadata["window"]["suite"], SUITE)
            self.assertEqual(metadata["window"]["plan"], PLAN)
            self.assertEqual(metadata["timing_basis"], "fixed qualification budget; workload duration not yet measured")

    def test_four_copy_record_and_canonical_series_keep_file_sets_distinct(self):
        from benchmarks import run as harness
        new = measurement(binding(SUITE))
        self.assertEqual([x["id"] for x in new["cases"]], ["bulk-100m-1024gb"])
        self.assertEqual(len(new["trials"]), 4)
        self.assertEqual({x["variant_id"] for x in new["trials"]}, {"rcp-default", "rcp-baseline"})
        self.assertEqual(new["context"]["verification"], common.verification_contract(SUITE))
        manifest = harness.load_manifest(ROOT / "benchmarks/large-cases.json")
        variant, = manifest["variants"]
        cases = {case["id"]: case for case in manifest["cases"]}
        contract = common.measurement_contract(SUITE)
        def series(case):
            return harness.series_id(case, variant, contract["cache_policy"], "local", contract["runner_label"],
                                     {}, {}, verification=common.verification_contract(SUITE))
        self.assertNotEqual(series(cases["bulk-1g-1tb"]), series(cases["bulk-100m-1024gb"]))
        self.assertEqual(series(cases["bulk-100m-1024gb"]), series(dict(cases["bulk-100m-1024gb"])))


if __name__ == "__main__":
    unittest.main()
