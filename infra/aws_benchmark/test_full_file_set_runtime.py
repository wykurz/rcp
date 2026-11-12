"""Full-set runtime guards with fake AWS, systemd and storage only; no large fixtures."""

import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

from . import common, durable, worker, large_storage
from .test_durable_contracts import NOW, request as legacy_request
from .test_durable_coordinator import Cloud
from .test_durable_worker import durable_fixture, request as worker_request
from .test_large_storage import PreparationFixture
from .test_production_stack import INSTANCE, PREFIX, launch_context, permits
from .test_worker_review import report

SUITE = common.FULL_FILE_SET_SUITE
PROFILE = common.FULL_RUNTIME_PROFILE


def full_request(created=NOW):
    old = legacy_request()
    identity = {key: old[key] for key in ("run", "baseline", "candidate", "controller")}
    return durable.request(dict(identity, suite=SUITE), created, old["sha256"], old["pins"],
                           runtime_seconds=28800, capacity_seconds=7200)


def full_window(old):
    value = copy.deepcopy(old)
    start = common.utc(value["attempt_started_at"])
    value.update(suite=SUITE, plan=common.durable_plan_for_suite(SUITE), runtime_seconds=28800,
        request_expires=common.timestamp(start + timedelta(hours=2)),
        outer_deadline=common.timestamp(start + timedelta(hours=10)),
        deadline=common.timestamp(start + timedelta(seconds=28500)))
    return value


class PlanTests(unittest.TestCase):
    def test_full_suite_has_immutable_fixed_budget_and_current_serial_case_order(self):
        value = full_request()
        contract = common.durable_plan_contract(value["plan"])
        self.assertEqual((contract["max_runtime"], contract["max_provisioning"], contract["max_attempts"]),
                         (28800, 7200, 120))
        self.assertEqual(contract["reservation_microusd"], 50_000_000)
        self.assertEqual(contract["runtime_profile"], PROFILE)
        self.assertEqual(durable.minimum_runtime(value["plan"]), 28800)
        self.assertEqual(common.durable_worker_reserve(value["plan"]), 28200)
        self.assertNotIn("work_seconds", value["plan"])
        self.assertIn("not yet measured", contract["timing_basis"])
        self.assertEqual(common.SUITES[SUITE], ("bulk-1g-1tb", "bulk-100m-1024gb",
                                              "bulk-1m-1024gb", "tiny-1k-10240k"))
        with self.assertRaises(TypeError):
            common._CURRENT_PLAN_VERSIONS[SUITE] = "bulk-metadata-plan-v1"
        old = legacy_request()
        self.assertEqual((old["runtime_seconds"], durable.CAPACITY_SECONDS, durable.MAX_ATTEMPTS),
                         (10800, 43200, 720))
        self.assertNotIn("reservation_microusd", common.durable_plan_contract(old["plan"]))

    def test_experimental_suites_have_no_legacy_admission_or_cross_suite_plan(self):
        for suite in (SUITE, "bulk-1m-metadata", "tiny-1k-metadata"):
            with self.subTest(suite=suite), self.assertRaises(ValueError):
                common.validate_admission(dict(legacy_request(), suite=suite))
        for runtime in (True, 28800.0, 28799, 28801):
            value = full_request()
            value["runtime_seconds"] = runtime
            with self.subTest(runtime=runtime), self.assertRaises(ValueError):
                durable.validate(value)
        value = full_request()
        value["suite"] = "bulk-100m-metadata"
        for exact in (True, False):
            with self.assertRaisesRegex(ValueError, "another file set"):
                durable.validate(value, exact=exact)
        value = full_request()
        value["capacity_deadline"] = common.timestamp(NOW + timedelta(seconds=7201))
        value["outer_deadline"] = common.timestamp(NOW + timedelta(seconds=36001))
        with self.assertRaisesRegex(ValueError, "capacity wait"):
            durable.validate(value)
        durable.attempt(full_request(), 120, NOW + timedelta(seconds=7140))
        with self.assertRaisesRegex(ValueError, "attempt bound"):
            durable.attempt(full_request(), 121, NOW)

    def test_monotonic_and_cost_ceilings_require_the_exact_explicit_full_plan(self):
        plan = full_request()["plan"]
        clock = {"wall": 1000, "monotonic": 100}
        with patch.object(common.time, "time", side_effect=lambda: clock["wall"]), \
             patch.object(common.time, "monotonic", side_effect=lambda: clock["monotonic"]):
            deadline = datetime.fromtimestamp(40000, timezone.utc)
            with self.assertRaises(ValueError):
                common.Budget(deadline, ceiling_seconds=28800)
            budget = common.Budget(deadline, ceiling_seconds=28800, plan=plan)
            clock.update(wall=0, monotonic=200)
            self.assertEqual(budget.left(), 28700)
        priced = durable.cost_projection(hourly="4.608", ancillary="1", runtime_seconds=28800,
                                          capacity_seconds=7200, plan=plan)
        self.assertEqual(priced["normal_usd"], "39.0928")
        with self.assertRaises(ValueError):
            durable.cost_projection(hourly="4.608", ancillary="1", runtime_seconds=28800)
        with self.assertRaises(ValueError):
            durable.cost_projection(hourly="4.608", ancillary="1", runtime_seconds=28800,
                                    capacity_seconds=7201, plan=plan)


class InstanceGuardTests(unittest.TestCase):
    def test_launch_profile_is_instance_only_and_known_binding_rejects_missing_or_extra_profile(self):
        cloud = Cloud()
        cloud.request = full_request()
        allocation = durable.attempt(cloud.request, 1, NOW)
        instance = cloud.instance(allocation=allocation)
        durable.validate_allocation_instance(cloud.request, instance, allocation)
        params = cloud.service.launch_parameters(cloud.request, allocation)
        specs = {entry["ResourceType"]: {t["Key"]: t["Value"] for t in entry["Tags"]}
                 for entry in params["TagSpecifications"]}
        self.assertEqual(specs["instance"][durable.TAG_RUNTIME_PROFILE], PROFILE)
        self.assertEqual(set(specs["volume"]), durable.TAG_KEYS)
        self.assertEqual(set(specs["instance"]), durable.TAG_KEYS | {durable.TAG_RUNTIME_PROFILE})
        instance["Tags"] = [t for t in instance["Tags"] if t["Key"] != durable.TAG_RUNTIME_PROFILE]
        with self.assertRaisesRegex(ValueError, "tags differ"):
            durable.validate_allocation_instance(cloud.request, instance, allocation)
        cloud.request = legacy_request()
        allocation = durable.attempt(cloud.request, 1, NOW)
        instance = cloud.instance(allocation=allocation)
        instance["Tags"].append({"Key": durable.TAG_RUNTIME_PROFILE, "Value": PROFILE})
        with self.assertRaisesRegex(ValueError, "tags differ"):
            durable.validate_allocation_instance(cloud.request, instance, allocation)

    def test_independent_scan_uses_only_instance_profile_with_ddb_reads_unavailable(self):
        cloud = Cloud()
        cloud.request = full_request()
        allocation = durable.attempt(cloud.request, 1, NOW)
        cloud.instances = [cloud.instance(allocation=allocation)]
        cloud.api["dynamodb"].get_item.side_effect = AssertionError("DDB read forbidden")
        cloud.now = NOW + timedelta(hours=4)
        cloud.service.independent_scan()
        cloud.api["ec2"].terminate_instances.assert_not_called()
        cloud.api["dynamodb"].get_item.assert_not_called()
        cloud.now = NOW + timedelta(hours=8)
        cloud.service.independent_scan()
        cloud.api["ec2"].terminate_instances.assert_called_once_with(InstanceIds=[cloud.instances[0]["InstanceId"]])

    def test_missing_unknown_or_overlong_profile_fails_closed_without_lease(self):
        for profile in (None, "unknown", PROFILE):
            cloud = Cloud()
            cloud.request = full_request()
            allocation = durable.attempt(cloud.request, 1, NOW)
            instance = cloud.instance(allocation=allocation)
            instance["Tags"] = [t for t in instance["Tags"] if t["Key"] != durable.TAG_RUNTIME_PROFILE]
            if profile is not None:
                instance["Tags"].append({"Key": durable.TAG_RUNTIME_PROFILE, "Value": profile})
            if profile == PROFILE:
                next(t for t in instance["Tags"] if t["Key"] == durable.TAG_DEADLINE)["Value"] = common.timestamp(NOW + timedelta(seconds=28801))
            cloud.instances = [instance]
            with self.subTest(profile=profile):
                cloud.service.independent_scan()
                cloud.api["ec2"].terminate_instances.assert_called_once()
                cloud.api["dynamodb"].get_item.assert_not_called()
        cloud = Cloud()
        allocation = durable.attempt(cloud.request, 1, NOW)
        instance = cloud.instance(allocation=allocation)
        self.assertEqual(durable.tagged_expiry(instance, cloud.config.namespace, NOW), NOW + timedelta(hours=3))

    def test_root_volume_recovery_filters_only_unchanged_five_volume_tags(self):
        cloud = Cloud()
        cloud.request = full_request()
        cloud.state = durable.initial_lease(cloud.request)
        allocation = durable.attempt(cloud.request, 1, NOW)
        cloud.state.update(attempt=allocation, known_instance_ids=["i-" + "1" * 17],
            seen={"i-" + "1" * 17: dict(root_volumes=[], state="terminated", launch_at=common.timestamp(NOW))})
        volume = dict(VolumeId="vol-" + "2" * 17,
            Tags=[{"Key": k, "Value": v} for k, v in durable.volume_tags(cloud.request, allocation).items()],
            Attachments=[])
        cloud.api["ec2"].describe_volumes.side_effect = lambda **kwargs: {"Volumes": [volume]}
        cloud.service.discover_roots(cloud.state)
        filters = cloud.api["ec2"].describe_volumes.call_args.kwargs["Filters"]
        self.assertEqual({x["Name"] for x in filters}, {"tag:" + x for x in durable.TAG_KEYS})
        self.assertEqual(cloud.state["seen"]["i-" + "1" * 17]["root_volumes"], [volume["VolumeId"]])

    def test_only_exact_optional_instance_tag_is_allowed_by_offline_iam_policy(self):
        base = launch_context()
        full = base | {"aws:RequestTag/rcp:runtime-profile": PROFILE,
                       "aws:TagKeys": base["aws:TagKeys"] + [durable.TAG_RUNTIME_PROFILE],
                       "ec2:CreateAction": "RunInstances"}
        volume = "arn:aws:ec2:us-east-1:123456789012:volume/vol-00000000000000001"
        for action in ("ec2:RunInstances", "ec2:CreateTags"):
            self.assertTrue(permits("CoordinatorRole", action, INSTANCE, full))
            self.assertFalse(permits("CoordinatorRole", action, INSTANCE,
                                     full | {"aws:RequestTag/rcp:runtime-profile": "unknown"}))
            self.assertFalse(permits("CoordinatorRole", action, volume,
                                     full | {"ec2:VolumeType": "gp3", "ec2:VolumeSize": 16, "ec2:Encrypted": "true"}))
        self.assertTrue(permits("CoordinatorRole", "ec2:RunInstances", INSTANCE, base))


class SupervisorTests(unittest.TestCase):
    def run_full(self, *, extension_error=None, notifier_present=True):
        with durable_fixture() as (root, cloud, measure, clock, identity, prepare):
            lease = full_window(json.loads(cloud.objects[common.WINDOW_KEY]))
            cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
            cloud.objects[common.REQUEST_KEY] = json.dumps(worker_request(lease)).encode()
            from unittest.mock import Mock
            notifier, events = Mock(), []
            notifier.ready.side_effect = lambda budget: events.append("ready")
            def imds(budget):
                self.assertEqual(events[-1], "ready")
                events.append("imds")
                return lease["instance_id"]
            identity.side_effect = imds
            def notify_extension(budget, **kwargs):
                self.assertTrue((root / "persistent/durable-started.json").exists())
                self.assertEqual(kwargs, dict(maximum_seconds=28800, shutdown_seconds=300))
                events.append("extend")
                if extension_error is not None:
                    raise extension_error
                return 28_708_000_000
            notifier.extend.side_effect = notify_extension
            original = worker.extend_supervisor_runtime
            def as_root(*args):
                with patch.object(worker.os, "geteuid", return_value=0):
                    return original(*args)
            with patch.object(worker, "supervisor_notifier", return_value=notifier if notifier_present else None), \
                 patch.object(worker, "extend_supervisor_runtime", side_effect=as_root):
                if not notifier_present:
                    identity.side_effect = None
                worker.run(cloud.api["s3"], "bucket", direct=True)
                first, _ = report(cloud)
                worker.run(cloud.api["s3"], "bucket", direct=True)
                second, _ = report(cloud)
            return first, second, events, notifier, measure.call_count, prepare.call_count, measure.call_args

    def test_full_worker_readies_before_io_and_consumes_once_before_extension(self):
        first, second, events, notifier, measurements, preparations, measured = self.run_full()
        self.assertTrue(first["status"]["success"])
        self.assertEqual(first["supervisor_runtime"]["extension_usec"], 28_708_000_000)
        self.assertGreater(first["command_timeout_seconds"], 7200)
        self.assertLessEqual(first["command_timeout_seconds"], 28800)
        self.assertTrue(first["supervisor_runtime"]["barrier_processed"])
        self.assertFalse(second["status"]["success"])
        self.assertEqual(second["status"]["error"]["type"], "FileExistsError")
        self.assertEqual(events, ["ready", "imds", "extend", "ready", "imds"])
        self.assertEqual(notifier.extend.call_count, 1)
        self.assertEqual((measurements, preparations), (1, 1))
        argv = measured.args[0]
        self.assertIn("--system-telemetry", argv)
        self.assertIn("--no-timings", argv)
        self.assertEqual(int(argv[argv.index("--timeout") + 1]), first["command_timeout_seconds"])
        self.assertIn("--property=RuntimeMaxSec=" + str(first["command_timeout_seconds"]), argv)
        self.assertEqual([argv[i + 1] for i, v in enumerate(argv) if v == "--case"], list(common.SUITES[SUITE]))
        old = worker.command(dict(suite="bulk-1g-metadata", baseline="a" * 40), 600)
        self.assertNotIn("--system-telemetry", old)
        self.assertEqual(old[old.index("--timeout") + 1], "600")

    def test_ordinary_notify_worker_readies_but_never_extends(self):
        from unittest.mock import Mock
        with durable_fixture() as (_, cloud, measure, _, _, _):
            notifier = Mock()
            with patch.object(worker, "supervisor_notifier", return_value=notifier):
                worker.run(cloud.api["s3"], "bucket", direct=True)
            metadata, _ = report(cloud)
            self.assertTrue(metadata["status"]["success"])
            self.assertGreater(metadata["command_timeout_seconds"], 1200)
            self.assertLessEqual(metadata["command_timeout_seconds"], 10800)
            notifier.ready.assert_called_once()
            notifier.extend.assert_not_called()
            measure.assert_called_once()

    def test_missing_context_or_failed_extension_never_starts_candidate(self):
        for error, context in ((ValueError("notification failed"), True), (None, False)):
            with self.subTest(context=context):
                first, _, _, _, measurements, preparations, _ = self.run_full(
                    extension_error=error, notifier_present=context)
                self.assertFalse(first["status"]["success"])
                self.assertEqual((measurements, preparations), (0, 0))

    def test_supervisor_requires_exact_request_identity_before_notification(self):
        value = full_request(datetime.now(timezone.utc) - timedelta(seconds=90))
        start = common.utc(value["created_at"])
        window = durable.window(value, dict(instance_id="i-" + "1" * 17,
            launch_at=common.timestamp(start + timedelta(seconds=30)), attempt_id=value["run"] + "-1",
            attempt_started_at=value["created_at"], deadline=common.timestamp(start + timedelta(seconds=28500))))
        request = worker_request(window)
        from unittest.mock import Mock
        for identity, changed in (("i-" + "2" * 17, request),
                                  (window["instance_id"], dict(request, suite="bulk-1g-metadata"))):
            notifier = Mock()
            with patch.object(worker.os, "geteuid", return_value=0):
                with self.assertRaises(ValueError):
                    worker.extend_supervisor_runtime(window, changed, identity, object(), notifier)
                notifier.extend.assert_not_called()


class FullStorageTests(PreparationFixture, unittest.TestCase):
    def test_arrays_are_prepared_once_and_each_serial_case_has_capacity_proof(self):
        cases = tuple(dict(self.case, id=name) for name in common.SUITES[SUITE])
        self.capacity.side_effect = [{"case": c["id"]} for c in cases]
        result = large_storage.prepare(self.data, cases, self.budget, 1000, 1000, self.execute)
        self.assertEqual(self.commands().count("mkfs.ext4"), 2)
        self.assertEqual(self.commands().count("mdadm"), 2)
        self.assertEqual([call.args[-1] for call in self.capacity.call_args_list], list(cases))
        self.assertEqual(set(result["capacity_by_case"]), set(common.SUITES[SUITE]))
        for side in large_storage.SIDES:
            self.assertEqual(result["arrays"][side]["member_models"], [large_storage.MODEL] * 2)
            self.assertEqual(result["arrays"][side]["member_bytes"], [900_000_000_000] * 2)

    def test_duplicate_case_identity_refuses_before_any_disk_command(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            large_storage.prepare(self.data, [dict(self.case, id="same")] * 2,
                                  self.budget, 1000, 1000, self.execute)
        self.assertFalse(self.calls)


class RetirementSourceTests(unittest.TestCase):
    def test_obsolete_runtime_role_has_only_deny_trust_and_deny_all_permissions(self):
        source = yaml.safe_load(Path(__file__).with_name("stack.yaml").read_text())
        role = source["Resources"]["RuntimeRole"]["Properties"]
        self.assertEqual([s["Effect"] for s in role["AssumeRolePolicyDocument"]["Statement"]], ["Deny"])
        self.assertEqual(role["Policies"], [{"PolicyName": "BenchmarkRuntime", "PolicyDocument": {
            "Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Action": "*", "Resource": "*"}]}}])
        self.assertEqual(role["MaxSessionDuration"], 7200)


if __name__ == "__main__":
    unittest.main()
