"""Offline direct-allocation and independent-janitor regressions; no AWS calls."""

import copy
import base64
from datetime import datetime, timedelta, timezone
import io
import json
import os
import unittest
from unittest.mock import Mock, patch

from infra.aws_benchmark import common, durable, durable_aws

NOW = datetime(2026, 10, 21, 12, tzinfo=timezone.utc)
INSTANCE = "i-" + "1" * 17
ROOT = "vol-" + "2" * 17


class ApiError(Exception):
    def __init__(self, code, request_id="request-123", status=500):
        super().__init__(code)
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"RequestId": request_id, "HTTPStatusCode": status}}


class ProcessDeath(BaseException):
    pass


def configuration():
    return durable_aws.Configuration(region="us-east-1", account="123456789012", namespace="benchmark", table="leases",
        bucket="bucket", lt_id="lt-" + "a" * 17, lt_version="9", ami="ami-" + "b" * 17, source="c" * 40,
        subnet="subnet-" + "d" * 17, security_group="sg-" + "e" * 17,
        profile_arn="arn:aws:iam::123456789012:instance-profile/worker", runtime_group="runtime",
        termination_role="arn:aws:iam::123456789012:role/terminate")


def launch_data(config):
    return dict(ImageId=config.ami, InstanceType="c5d.24xlarge", IamInstanceProfile={"Arn": config.profile_arn},
        MetadataOptions=dict(HttpTokens="required", HttpPutResponseHopLimit=1, HttpEndpoint="enabled", InstanceMetadataTags="disabled"),
        BlockDeviceMappings=[dict(DeviceName="/dev/sda1", Ebs=dict(VolumeType="gp3", VolumeSize=16, Encrypted=True,
                                                                 KmsKeyId="alias/aws/ebs", DeleteOnTermination=True))],
        NetworkInterfaces=[dict(DeviceIndex=0, AssociatePublicIpAddress=True, DeleteOnTermination=True, Groups=[config.security_group])],
        InstanceInitiatedShutdownBehavior="terminate", UserData="")


class Cloud:
    def __init__(self):
        self.now = NOW
        self.config = configuration()
        self.binary = b"identical opaque binary"
        self.request = durable.request(dict(run="123-1", baseline="a" * 40, candidate="a" * 40,
            suite="bulk-1g-metadata", controller="d" * 40), NOW,
            {key: common.digest(self.binary) for key in common.BINARIES}, self.config.pins())
        self.state, self.revision = durable.initial_lease(self.request), 0
        self.guard = dict(namespace=self.config.namespace, observed_at=common.timestamp(NOW), healthy=True)
        self.instances, self.schedules, self.events, self.launches = [], {}, [], []
        self.objects = {"input/" + key: self.binary for key in common.BINARIES}
        self.objects[common.STATUS_KEY] = json.dumps(common.pending_status(self.request["run"])).encode()
        self.launch_error, self.allocate_on_error, self.update_hook, self.get_hook = None, False, None, None
        self.schedule_error, self.schedule_store = None, True
        self.api = {key: Mock() for key in ("dynamodb", "ec2", "scheduler", "s3")}
        self.api["dynamodb"].get_item.side_effect = self.get_item
        self.api["dynamodb"].put_item.side_effect = self.put_item
        self.api["dynamodb"].update_item.side_effect = self.update_item
        self.api["ec2"].describe_instances.side_effect = lambda **kw: {"Reservations": [{"Instances": copy.deepcopy(self.instances)}]}
        self.api["ec2"].describe_launch_template_versions.return_value = {"LaunchTemplateVersions": [
            {"VersionNumber": 9, "LaunchTemplateData": launch_data(self.config)}]}
        self.api["ec2"].run_instances.side_effect = self.run_instances
        self.api["ec2"].terminate_instances.side_effect = lambda **kw: self.events.append(("terminate", kw["InstanceIds"]))
        self.api["ec2"].describe_volumes.side_effect = self.describe_volumes
        self.api["scheduler"].get_schedule.side_effect = self.get_schedule
        self.api["scheduler"].create_schedule.side_effect = self.create_schedule
        self.api["s3"].head_object.side_effect = self.head_object
        self.api["s3"].get_object.side_effect = self.get_object
        self.api["s3"].put_object.side_effect = self.put_object
        self.sleeps = []
        self.service = durable_aws.Service(self.api, self.config, now=lambda: self.now, sleep=self.sleeps.append)
        self.event = dict(run="123-1", execution_name="123-1",
            execution_arn="arn:aws:states:us-east-1:123456789012:execution:capacity:123-1")

    def get_item(self, **kw):
        if kw["Key"]["pk"]["S"] == "guard":
            return {} if self.guard is None else {"Item": {"payload": {"S": json.dumps(self.guard)}}}
        return {"Item": {"payload": {"S": json.dumps(self.state)}, "revision": {"N": str(self.revision)}}}

    def put_item(self, **kw):
        self.events.append(("guard-write", None))
        self.guard = json.loads(kw["Item"]["payload"]["S"])

    def update_item(self, **kw):
        if self.update_hook:
            hook, self.update_hook = self.update_hook, None
            hook()
        values = kw["ExpressionAttributeValues"]
        if int(values[":previous"]["N"]) != self.revision:
            raise ApiError("ConditionalCheckFailedException")
        self.state = json.loads(values[":payload"]["S"])
        self.revision += 1
        self.events.append(("save", self.state["phase"]))

    def instance(self, identity=INSTANCE, *, allocation=None, state="running"):
        allocation = allocation or self.state["attempt"]
        return dict(InstanceId=identity, LaunchTime=common.utc(allocation["started_at"]),
            ImageId=self.config.ami, InstanceType="c5d.24xlarge", ClientToken=allocation["token"],
            SubnetId=self.config.subnet, SecurityGroups=[{"GroupId": self.config.security_group}],
            IamInstanceProfile={"Arn": self.config.profile_arn}, RootDeviceName="/dev/sda1",
            BlockDeviceMappings=[{"DeviceName": "/dev/sda1", "Ebs": {"VolumeId": ROOT, "DeleteOnTermination": True}}],
            State={"Name": state}, Tags=[{"Key": k, "Value": v} for k, v in durable.tags(self.request, allocation).items()])

    def run_instances(self, **kw):
        self.launches.append(copy.deepcopy(kw))
        self.events.append(("launch", self.state["phase"]))
        if self.launch_error is None or self.allocate_on_error:
            self.instances = [self.instance()]
        if self.launch_error is not None:
            raise self.launch_error
        return {"Instances": copy.deepcopy(self.instances)}

    def get_schedule(self, **kw):
        key = (kw["GroupName"], kw["Name"])
        if key not in self.schedules:
            raise ApiError("ResourceNotFoundException", status=400)
        return copy.deepcopy(self.schedules[key])

    def create_schedule(self, **kw):
        self.events.append(("schedule", kw["Name"]))
        value = {k: v for k, v in kw.items() if k != "ClientToken"}
        if self.schedule_store:
            self.schedules[(kw["GroupName"], kw["Name"])] = value
        if self.schedule_error:
            raise self.schedule_error
        return {}

    def describe_volumes(self, **kw):
        if "VolumeIds" in kw:
            raise ApiError("InvalidVolume.NotFound", status=400)
        return {"Volumes": []}

    def head_object(self, **kw):
        if self.get_hook:
            hook, self.get_hook = self.get_hook, None
            hook()
        data = self.objects[kw["Key"]]
        return dict(ContentLength=len(data), ChecksumType="FULL_OBJECT",
                    ChecksumSHA256=base64.b64encode(bytes.fromhex(common.digest(data))).decode("ascii"))

    def get_object(self, **kw):
        if self.get_hook:
            hook, self.get_hook = self.get_hook, None
            hook()
        data = self.objects[kw["Key"]]
        return {"Body": io.BytesIO(data), "ContentLength": len(data)}

    def put_object(self, **kw):
        self.objects[kw["Key"]] = kw["Body"]
        self.events.append(("object", kw["Key"]))

    def coordinator(self):
        return self.service.coordinator(self.event)

    def handler(self, name):
        with patch.object(durable_aws.Configuration, "environment", return_value=self.config), \
             patch.object(durable_aws, "clients", return_value=self.api), \
             patch.object(durable_aws, "Service", return_value=self.service):
            return getattr(durable_aws, name)(self.event if name == "coordinator" else {}, None)


class ContractTests(unittest.TestCase):
    def test_capacity_and_allocation_clocks_are_separate(self):
        c = Cloud()
        allocation = durable.attempt(c.request, 2, NOW + timedelta(hours=11))
        instance = c.instance(allocation=allocation)
        instance["LaunchTime"] += timedelta(seconds=60)
        binding = durable.bind(c.request, instance, allocation)
        self.assertEqual(common.utc(binding["close_at"]), NOW + timedelta(hours=14))
        self.assertEqual(common.utc(c.request["outer_deadline"]), NOW + timedelta(hours=15))
        self.assertEqual(durable.window(c.request, binding)["opens"], allocation["started_at"])

    def test_late_or_foreign_instance_cannot_acquire_fresh_runtime(self):
        for change in ({"ClientToken": "wrong"}, {"SubnetId": "subnet-wrong"},
                       {"LaunchTime": NOW - timedelta(seconds=1)}, {"LaunchTime": NOW + timedelta(hours=12)}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                c = Cloud()
                allocation = durable.attempt(c.request, 1, NOW)
                durable.bind(c.request, {**c.instance(allocation=allocation), **change}, allocation)

    def test_budget_prices_one_allocation_and_sixteen_minute_margin(self):
        cost = durable.cost_projection(hourly="4.608", ancillary="2")
        self.assertEqual(cost["guarded_failure_usd"], "17.0528")
        self.assertEqual(cost["capacity_seconds"], 43200)
        with self.assertRaises(ValueError):
            durable.cost_projection(hourly="4.608", ancillary=0, shutdown_seconds=959)

    def test_invalid_prices_or_runtime_cannot_pass_admission(self):
        for change in ({"hourly": "NaN"}, {"hourly": "Infinity"}, {"ancillary": -1}, {"runtime_seconds": 3600}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                durable.cost_projection(**{**dict(hourly="4.608", ancillary=0), **change})

    def test_mismatched_aa_bytes_are_rejected(self):
        c = Cloud()
        c.request["sha256"]["candidate/rcp"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "A/A binary digests differ"):
            durable.validate(c.request)

    def test_all_capacity_receipts_fit_the_lease_bound(self):
        c = Cloud()
        c.state["capacity_failures"] = [dict(attempt=i, at=common.timestamp(NOW), request_id="a" * 64)
                                       for i in range(1, durable.MAX_ATTEMPTS + 1)]
        c.state["attempt"] = durable.attempt(c.request, 720, NOW)
        c.state["attempt"]["parameters"] = c.service.launch_parameters(c.request, c.state["attempt"])
        self.assertLess(len(json.dumps(c.state, separators=(",", ":")).encode()), 100_000)


class LaunchTests(unittest.TestCase):
    def test_persisted_single_allocation_precedes_run_and_tags_both_resources(self):
        c = Cloud()
        c.coordinator()
        self.assertEqual(c.events[:2], [("save", "launching"), ("launch", "launching")])
        params = c.launches[0]
        self.assertEqual(params, c.state["attempt"]["parameters"])
        self.assertEqual((params["MinCount"], params["MaxCount"]), (1, 1))
        self.assertEqual(params["NetworkInterfaces"][0]["SubnetId"], c.config.subnet)
        self.assertEqual({t["ResourceType"] for t in params["TagSpecifications"]}, {"instance", "volume"})
        self.assertEqual(params["TagSpecifications"][0]["Tags"], params["TagSpecifications"][1]["Tags"])
        c.coordinator()
        self.assertEqual(len(c.launches), 1)

    def test_definitive_capacity_failure_allows_only_a_delayed_new_attempt(self):
        c = Cloud()
        c.launch_error = ApiError("InsufficientInstanceCapacity")
        c.coordinator()
        first = copy.deepcopy(c.state["attempt"])
        self.assertFalse(c.state["allocation_consumed"])
        self.assertEqual(c.state["capacity_failures"][0]["request_id"], "request-123")
        c.coordinator()
        self.assertEqual(len(c.launches), 1)
        c.now += timedelta(seconds=60)
        c.launch_error = None
        c.coordinator()
        self.assertEqual(len(c.launches), 2)
        self.assertNotEqual(first["token"], c.state["attempt"]["token"])
        self.assertEqual(c.state["attempts"], 2)
        self.assertTrue(c.state["allocation_consumed"])

    def test_timeout_internal_error_and_unproven_capacity_error_consume_allocation(self):
        for error in (TimeoutError(), ApiError("InternalError"), ApiError("InsufficientInstanceCapacity", request_id=None)):
            with self.subTest(error=type(error).__name__):
                c = Cloud()
                c.launch_error = error
                c.coordinator()
                allocation = copy.deepcopy(c.state["attempt"])
                c.now += timedelta(hours=1)
                c.coordinator()
                self.assertEqual(len(c.launches), 3)
                self.assertTrue(all(parameters == c.launches[0] for parameters in c.launches))
                self.assertEqual(c.state["attempt"], allocation)
                self.assertTrue(c.state["allocation_consumed"])
                self.assertEqual(c.state["launch_outcome"], "uncertain")

    def test_lost_response_with_instance_visible_never_launches_a_replacement(self):
        c = Cloud()
        c.launch_error, c.allocate_on_error = TimeoutError(), True
        c.coordinator()
        c.service.watchdog()
        self.assertNotIn(("terminate", [INSTANCE]), c.events)
        c.coordinator()
        self.assertEqual(len(c.launches), 3)
        self.assertTrue(all(parameters == c.launches[0] for parameters in c.launches))
        self.assertTrue(c.state["worker_published"])
        self.assertEqual(c.state["launch_outcome"], "accepted")

    def test_process_death_after_intent_is_not_replayed(self):
        c = Cloud()
        c.launch_error = ProcessDeath()
        with self.assertRaises(ProcessDeath):
            c.coordinator()
        c.now += timedelta(seconds=durable.LAUNCH_ACK_SECONDS + 1)
        c.service.watchdog()
        c.coordinator()
        self.assertEqual(len(c.launches), 1)
        self.assertIn("acknowledgement expired", c.state["close_reason"])

    def test_stale_or_missing_guard_never_sends_runinstances(self):
        for guard in (None, dict(namespace="benchmark", healthy=True, observed_at=common.timestamp(NOW - timedelta(seconds=121)))):
            c = Cloud()
            c.guard = guard
            self.assertEqual(c.handler("coordinator")["state"], "wait")
            self.assertEqual(c.launches, [])

    def test_guard_must_still_be_fresh_after_artifact_reads(self):
        c = Cloud()
        c.get_hook = lambda: setattr(c, "now", NOW + timedelta(seconds=121))
        self.assertEqual(c.handler("coordinator")["state"], "wait")
        self.assertEqual(c.launches, [])

    def test_watchdog_progress_during_artifact_reads_defers_without_failure(self):
        c = Cloud()
        def progress():
            c.revision += 1
        c.get_hook = progress
        self.assertEqual(c.coordinator()["state"], "wait")
        self.assertEqual(c.launches, [])
        c.coordinator()
        self.assertEqual(len(c.launches), 1)

    def test_intent_cas_contention_does_not_launch(self):
        c = Cloud()
        c.update_hook = lambda: setattr(c, "revision", c.revision + 1)
        self.assertEqual(c.handler("coordinator")["state"], "wait")
        self.assertEqual(c.launches, [])

    def test_delayed_intent_write_cannot_launch_after_freshness_expires(self):
        c = Cloud()
        c.update_hook = lambda: setattr(c, "now", NOW + timedelta(seconds=121))
        self.assertEqual(c.coordinator()["state"], "wait")
        self.assertEqual(c.launches, [])
        self.assertFalse(c.state["allocation_consumed"])
        self.assertEqual(c.state["launch_outcome"], "not-sent")
        self.assertEqual(c.state["phase"], "armed")

    def test_candidate_binary_mismatch_never_launches(self):
        c = Cloud()
        c.objects["input/candidate/rcp"] = b"different"
        with self.assertRaisesRegex(ValueError, "binary digest"):
            c.coordinator()
        self.assertEqual(c.launches, [])

    def test_capacity_deadline_cannot_be_renewed(self):
        c = Cloud()
        c.now += timedelta(hours=12)
        c.coordinator()
        self.assertEqual(c.launches, [])
        self.assertEqual(c.state["phase"], "closing")


class JanitorTests(unittest.TestCase):
    def test_schedule_is_read_back_before_worker_publication(self):
        c = Cloud()
        c.coordinator()
        c.service.watchdog()
        events = [e[0] for e in c.events]
        self.assertLess(events.index("schedule"), events.index("object"))
        self.assertTrue(c.state["runtime_verified"])
        target = next(iter(c.schedules.values()))["Target"]
        self.assertEqual(json.loads(target["Input"]), {"InstanceIds": [INSTANCE]})
        self.assertIn("ec2:terminateInstances", target["Arn"])

    def test_lost_schedule_response_reconciles_same_deadline(self):
        c = Cloud()
        c.coordinator()
        c.schedule_error = TimeoutError()
        c.service.watchdog()
        self.assertTrue(c.state["runtime_verified"])
        self.assertEqual(len(c.schedules), 1)

    def test_transient_schedule_create_with_missing_readback_defers_without_admission(self):
        c = Cloud()
        c.coordinator()
        c.schedule_error, c.schedule_store = TimeoutError(), False
        self.assertEqual(c.service.watchdog()["state"], "deferred")
        self.assertFalse(c.state["worker_published"])
        self.assertNotIn(("terminate", [INSTANCE]), c.events)
        c.schedule_error, c.schedule_store = None, True
        c.service.watchdog()
        self.assertTrue(c.state["worker_published"])
        calls = c.api["scheduler"].create_schedule.call_args_list
        self.assertEqual(calls[0], calls[1])

    def test_existing_later_cutoff_cannot_be_overwritten(self):
        c = Cloud()
        c.coordinator()
        binding = durable.bind(c.request, c.instances[0], c.state["attempt"])
        schedule = c.service.schedule(binding=binding)
        schedule["ScheduleExpression"] = "at(2099-01-01T00:00:00)"
        c.schedules[(schedule["GroupName"], schedule["Name"])] = schedule
        with self.assertRaises(ValueError):
            c.service.watchdog()
        c.api["scheduler"].create_schedule.assert_not_called()
        self.assertIn(("terminate", [INSTANCE]), c.events)

    def test_expired_instance_is_terminated_before_any_dynamodb_access(self):
        c = Cloud()
        c.coordinator()
        c.now += timedelta(hours=4)
        c.events.clear()
        c.api["dynamodb"].put_item.side_effect = RuntimeError("DynamoDB unavailable")
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            c.service.watchdog()
        self.assertEqual(c.events, [("terminate", [INSTANCE])])

    def test_missing_malformed_or_excessive_deadlines_fail_closed_without_lease(self):
        for mutation in (lambda tags: tags.pop(durable.TAG_DEADLINE),
                         lambda tags: tags.update({durable.TAG_DEADLINE: "invalid"}),
                         lambda tags: tags.update({durable.TAG_DEADLINE: "9999-12-31T23:59:59-12:00"}),
                         lambda tags: tags.update({durable.TAG_CREATED: "0001-01-01T00:00:00+12:00"}),
                         lambda tags: tags.update({durable.TAG_DEADLINE: common.timestamp(NOW + timedelta(hours=4))}),
                         lambda tags: tags.update({durable.TAG_CREATED: common.timestamp(NOW + timedelta(minutes=1))})):
            with self.subTest(mutation=mutation):
                c = Cloud()
                allocation = durable.attempt(c.request, 1, NOW)
                i = c.instance(allocation=allocation)
                tags = durable.instance_tags(i)
                mutation(tags)
                i["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
                c.instances = [i]
                c.api["dynamodb"].get_item.side_effect = RuntimeError("lease unavailable")
                with self.assertRaises(RuntimeError):
                    c.service.watchdog()
                self.assertIn(("terminate", [INSTANCE]), c.events)

    def test_unknown_namespace_is_never_touched(self):
        c = Cloud()
        allocation = durable.attempt(c.request, 1, NOW)
        i = c.instance(allocation=allocation)
        i["Tags"] = [{"Key": durable.TAG_NAMESPACE, "Value": "someone-else"}]
        c.instances = [i]
        c.service.independent_scan()
        c.api["ec2"].terminate_instances.assert_not_called()

    def test_multiple_managed_instances_are_all_terminated(self):
        c = Cloud()
        c.coordinator()
        c.instances.append(c.instance("i-" + "9" * 17))
        c.service.watchdog()
        self.assertEqual(set(tuple(e[1]) for e in c.events if e[0] == "terminate"), {(INSTANCE,), ("i-" + "9" * 17,)})
        self.assertFalse(c.state["worker_published"])

    def test_one_failed_termination_does_not_skip_other_managed_instances(self):
        c = Cloud()
        c.coordinator()
        second = "i-" + "9" * 17
        c.instances.append(c.instance(second))
        calls = []
        def terminate(**kw):
            calls.append(kw["InstanceIds"])
            if kw["InstanceIds"] == [INSTANCE]:
                raise TimeoutError("first termination uncertain")
        c.api["ec2"].terminate_instances.side_effect = terminate
        with self.assertRaises(TimeoutError):
            c.service.independent_scan()
        self.assertEqual(calls, [[INSTANCE], [second]])
        c.api["dynamodb"].put_item.assert_not_called()

    def test_restarted_instance_does_not_renew_attempt_deadline(self):
        c = Cloud()
        c.coordinator()
        c.service.watchdog()
        original = c.state["binding"]["close_at"]
        c.now += timedelta(seconds=60)
        c.instances[0]["LaunchTime"] = c.now
        c.service.watchdog()
        self.assertEqual(c.state["binding"]["close_at"], original)
        self.assertIn("restarted", c.state["close_reason"])

    def test_scan_failure_does_not_publish_fresh_heartbeat(self):
        c = Cloud()
        c.api["ec2"].describe_instances.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            c.service.watchdog()
        c.api["dynamodb"].put_item.assert_not_called()

    def test_runtime_disabled_does_not_disable_janitor(self):
        c = Cloud()
        environment = {"RCP_DURABLE_" + key.upper(): getattr(c.config, key) for key in c.config.__dataclass_fields__}
        with patch.dict(os.environ, environment | {"RCP_DURABLE_ENABLED": "false"}):
            self.assertEqual(durable_aws.Configuration.environment(), c.config)
            with self.assertRaisesRegex(ValueError, "disabled"):
                durable_aws.Configuration.environment(runtime=True)


class CleanupTests(unittest.TestCase):
    def closed(self):
        c = Cloud()
        c.coordinator()
        c.service.watchdog()
        c.instances[0]["State"]["Name"] = "terminated"
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        return c

    def test_unknown_allocation_is_never_cleared_by_empty_describe(self):
        c = Cloud()
        c.launch_error = TimeoutError()
        c.coordinator()
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(hours=1)
        with self.assertRaisesRegex(ValueError, "empty Describe"):
            c.service.watchdog()
        self.assertIsNone(c.state["cleanup"])
        c.coordinator()
        self.assertEqual(len(c.launches), 3)
        self.assertTrue(all(parameters == c.launches[0] for parameters in c.launches))

    def test_missing_retained_or_unreadable_roots_hold_the_lease(self):
        for kind in ("missing", "retained", "unreadable"):
            with self.subTest(kind=kind):
                c = self.closed()
                if kind == "missing":
                    c.instances[0]["BlockDeviceMappings"] = []
                    c.state["seen"][INSTANCE]["root_volumes"] = []
                elif kind == "retained":
                    c.api["ec2"].describe_volumes.side_effect = None
                    c.api["ec2"].describe_volumes.return_value = {"Volumes": [{"VolumeId": ROOT}]}
                else:
                    c.api["ec2"].describe_volumes.side_effect = TimeoutError()
                if kind == "unreadable":
                    self.assertEqual(c.service.watchdog()["state"], "deferred")
                else:
                    with self.assertRaises(ValueError):
                        c.service.watchdog()
                self.assertIsNone(c.state["cleanup"])

    def test_actual_root_absence_completes_without_releasing_lease(self):
        c = self.closed()
        c.service.watchdog()
        self.assertEqual(c.state["phase"], "complete")
        self.assertEqual(c.state["cleanup"]["deleted_roots"], [ROOT])
        self.assertFalse(c.state["cleanup"]["lease_released"])
        self.assertFalse(c.state["cleanup"]["unresolved_allocation"])
        self.assertEqual(c.state["cleanup"]["live_instances"], [])

    def test_disappearing_known_instance_is_not_assumed_terminated(self):
        c = self.closed()
        c.instances = []
        with self.assertRaisesRegex(ValueError, "not terminated"):
            c.service.watchdog()
        self.assertIsNone(c.state["cleanup"])

    def test_only_definitive_capacity_rejections_can_complete_with_no_instance(self):
        c = Cloud()
        c.launch_error = ApiError("InsufficientInstanceCapacity")
        c.coordinator()
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        c.service.watchdog()
        self.assertEqual(c.state["phase"], "complete")
        self.assertFalse(c.state["cleanup"]["allocation_consumed"])

    def test_original_failure_reason_survives_later_expiry(self):
        c = self.closed()
        c.state["close_reason"] = "original qualification failure"
        c.state["phase"] = "closing"
        c.service.watchdog()
        self.assertEqual(c.state["close_reason"], "original qualification failure")


if __name__ == "__main__":
    unittest.main()
