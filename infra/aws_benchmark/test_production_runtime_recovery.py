"""Repeated offline ticks preserve ambiguous allocation and immutable output evidence."""

import copy
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

from . import common, durable
from .durable_aws import AllocationDeferred
from .test_durable_coordinator import ApiError, INSTANCE, ROOT
from .test_production_runtime import ProductionCloud


class RuntimeRecoveryTests(unittest.TestCase):
    def setUp(self):
        network = patch("socket.create_connection", side_effect=AssertionError("offline tests only"))
        network.start()
        self.addCleanup(network.stop)

    def uncertain(self, *, allocated=False):
        cloud = ProductionCloud()
        cloud.launch_error, cloud.allocate_on_error = TimeoutError("lost launch response"), allocated
        cloud.coordinator()
        self.assertEqual(cloud.state["launch_outcome"], "uncertain")
        return cloud

    def fresh(self, cloud):
        cloud.guard["observed_at"] = common.timestamp(cloud.now)

    def terminal_horizon(self, cloud):
        cloud.now = common.utc(cloud.request["outer_deadline"]) + timedelta(
            seconds=durable.DELIVERY_DRAIN_SECONDS)

    def test_lost_intent_and_all_readbacks_recover_the_original_attempt(self):
        cloud = ProductionCloud()
        write, read = cloud.database.transact_write_items, cloud.database.transact_get_items
        unavailable = False
        def put(**parameters):
            nonlocal unavailable
            result = write(**parameters)
            unavailable = True
            raise TimeoutError("intent committed but reply lost")
        def get(**parameters):
            if unavailable:
                raise TimeoutError("readback unavailable for whole invocation")
            return read(**parameters)
        cloud.api["dynamodb"].transact_write_items.side_effect = put
        cloud.api["dynamodb"].transact_get_items.side_effect = get
        with self.assertRaises(TimeoutError):
            cloud.coordinator()
        cloud.api["dynamodb"].transact_write_items.side_effect = write
        cloud.api["dynamodb"].transact_get_items.side_effect = read
        original = copy.deepcopy(cloud.state["attempt"])
        self.assertEqual(cloud.state["phase"], "launching")
        self.assertEqual(cloud.launches, [])
        cloud.now += timedelta(seconds=100)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "launching")
        cloud.coordinator()
        self.assertEqual(cloud.launches, [original["parameters"]])
        self.assertEqual(cloud.state["attempt"], original)
        self.assertEqual(cloud.state["attempts"], 1)
        self.assertTrue(cloud.state["launch_replayed"])
        self.assertEqual(cloud.state["launch_outcome"], "accepted")

    def test_lost_capacity_checkpoint_recovers_same_token_not_new_allocation(self):
        cloud = ProductionCloud()
        write = cloud.database.transact_write_items
        def put(**parameters):
            for item in parameters["TransactItems"]:
                values = item.get("Update", {}).get("ExpressionAttributeValues", {})
                if ":state" in values and json.loads(values[":state"]["S"])["launch_outcome"] == "capacity-rejected":
                    raise TimeoutError("capacity proof not committed")
            return write(**parameters)
        cloud.api["dynamodb"].transact_write_items.side_effect = put
        cloud.launch_error = ApiError("InsufficientInstanceCapacity", status=400)
        with self.assertRaises(TimeoutError):
            cloud.coordinator()
        original = copy.deepcopy(cloud.state["attempt"])
        self.assertEqual(cloud.state["launch_outcome"], "uncertain")
        cloud.api["dynamodb"].transact_write_items.side_effect = write
        cloud.launch_error = None
        cloud.coordinator()
        self.assertEqual(cloud.launches, [original["parameters"], original["parameters"]])
        self.assertEqual(cloud.state["attempts"], 1)
        self.assertEqual(cloud.state["attempt"], original)
        self.assertEqual(cloud.state["launch_outcome"], "accepted")

    def test_delayed_negative_sender_cannot_erase_replay_uncertainty(self):
        for negative in ("not-sent", "capacity-rejected"):
            with self.subTest(negative=negative):
                cloud = self.uncertain()
                original = copy.deepcopy(cloud.state["attempt"])
                def delayed(**parameters):
                    outcome = dict(not_sent=True) if negative == "not-sent" else dict(
                        failure=ApiError("InsufficientInstanceCapacity", status=400))
                    cloud.service.finish_launch("123-1", original, **outcome)
                    self.assertEqual(cloud.state["launch_outcome"], "uncertain")
                    self.assertTrue(cloud.state["allocation_consumed"])
                    self.assertTrue(cloud.state["launch_replayed"])
                    return cloud.run_instances(**parameters)
                cloud.api["ec2"].run_instances.side_effect = delayed
                cloud.launch_error = None
                cloud.coordinator()
                self.assertEqual(cloud.state["launch_outcome"], "accepted")
                self.assertEqual(cloud.state["attempt"], original)
                self.assertEqual(cloud.state["attempts"], 1)
                self.assertTrue(all(value == original["parameters"] for value in cloud.launches))

    def test_later_capacity_rejection_cannot_resolve_prior_unknown_or_release_lease(self):
        cloud = self.uncertain()
        original = copy.deepcopy(cloud.state["attempt"])
        cloud.launch_error = ApiError("InsufficientInstanceCapacity", status=400)
        cloud.coordinator()
        self.assertEqual(cloud.state["launch_outcome"], "uncertain")
        self.assertEqual(cloud.state["capacity_failures"], [])
        self.assertTrue(cloud.state["allocation_consumed"])
        self.assertEqual(cloud.state["attempt"], original)
        self.terminal_horizon(cloud)
        with self.assertRaisesRegex(ValueError, "uncertain allocation"):
            cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "closing")
        self.assertIsNone(cloud.state["cleanup"])
        before = len(cloud.launches)
        cloud.coordinator()
        self.assertEqual(len(cloud.launches), before)

    def test_replay_requires_original_time_guard_and_unchanged_parameters(self):
        for cause in ("deadline", "stale-guard", "policy", "observed", "changed-parameters"):
            with self.subTest(cause=cause):
                cloud = self.uncertain()
                original = copy.deepcopy(cloud.state["attempt"])
                if cause == "deadline":
                    cloud.now = common.utc(original["close_at"]) - timedelta(
                        seconds=durable.minimum_runtime(cloud.request["plan"]) - 1)
                    self.fresh(cloud)
                elif cause == "stale-guard":
                    cloud.now += timedelta(seconds=121)
                elif cause == "policy":
                    cloud.database.items["policy"]["enabled"] = {"BOOL": False}
                elif cause == "observed":
                    cloud.instances = [cloud.instance()]
                else:
                    state, revision = cloud.service.load()
                    state["attempt"]["parameters"]["ClientToken"] = "different"
                    cloud.service.save(state, revision)
                before = len(cloud.launches)
                try:
                    cloud.coordinator()
                except (ValueError, RuntimeError, AllocationDeferred):
                    pass
                self.assertEqual(len(cloud.launches), before)
                self.assertEqual(cloud.state["attempts"], 1)

    def test_lost_retry_ack_does_not_send_and_later_tick_requires_new_ack(self):
        cloud = self.uncertain()
        original = cloud.owner.save_launch_retry
        def lost(state, revision):
            original(state, revision)
            raise TimeoutError("committed retry but lost acknowledgement")
        before = len(cloud.launches)
        with patch.object(cloud.owner, "save_launch_retry", side_effect=lost), self.assertRaises(TimeoutError):
            cloud.coordinator()
        self.assertEqual(len(cloud.launches), before)
        self.assertTrue(cloud.state["launch_replayed"])
        cloud.launch_error = None
        cloud.coordinator()
        self.assertEqual(len(cloud.launches), before + 1)
        self.assertEqual(cloud.state["attempts"], 1)

    def test_transient_policy_error_does_not_expire_accepted_runtime(self):
        for near_capacity in (False, True):
            with self.subTest(near_capacity=near_capacity):
                cloud = ProductionCloud()
                if near_capacity:
                    cloud.now = common.utc(cloud.request["capacity_deadline"]) - timedelta(seconds=10)
                    self.fresh(cloud)
                cloud.launch_error, cloud.allocate_on_error = TimeoutError(), True
                cloud.coordinator()
                original = copy.deepcopy(cloud.state["attempt"])
                cloud.now += timedelta(seconds=100)
                def get(**parameters):
                    if parameters["Key"]["pk"]["S"] == "policy":
                        raise TimeoutError("policy temporarily unavailable")
                    return cloud.production_get(**parameters)
                cloud.api["dynamodb"].get_item.side_effect = get
                self.assertEqual(cloud.service.watchdog()["state"], "deferred")
                self.assertEqual(cloud.state["launch_outcome"], "accepted")
                self.assertIsNone(cloud.state["close_reason"])
                self.assertIsNone(cloud.state["binding"])
                self.assertNotIn(("terminate", [INSTANCE]), cloud.events)
                cloud.api["dynamodb"].get_item.side_effect = cloud.production_get
                cloud.service.watchdog()
                self.assertTrue(cloud.state["worker_published"])
                self.assertEqual(cloud.state["attempt"], original)
                cloud.api["dynamodb"].get_item.side_effect = get
                cloud.now = common.utc(original["close_at"])
                self.assertEqual(cloud.service.watchdog()["state"], "deferred")
                self.assertEqual(cloud.state["phase"], "closing")
                self.assertIn(("terminate", [INSTANCE]), cloud.events)

    def test_exact_not_found_plus_root_deletion_recovers_missed_termination(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        cloud.instances = []
        def describe(**parameters):
            if "InstanceIds" in parameters:
                self.assertEqual(parameters["InstanceIds"], [INSTANCE])
                raise ApiError("InvalidInstanceID.NotFound", status=400)
            return dict(Reservations=[])
        cloud.api["ec2"].describe_instances.side_effect = describe
        self.terminal_horizon(cloud)
        cloud.now -= timedelta(seconds=1)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "closing")
        self.assertFalse(any("InstanceIds" in call.kwargs for call in cloud.api["ec2"].describe_instances.call_args_list))
        cloud.now += timedelta(seconds=1)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "complete")
        self.assertEqual(cloud.state["seen"][INSTANCE]["state"], "terminated")
        self.assertEqual(cloud.state["cleanup"]["deleted_roots"], [ROOT])
        self.assertEqual(cloud.service.watchdog()["state"], "complete")
        self.assertEqual(len(cloud.launches), 1)

    def test_exact_instance_or_root_ambiguity_remains_held(self):
        for cause in ("denied", "empty", "running", "wrong-id", "root-present", "root-denied", "missing-root", "paginated"):
            with self.subTest(cause=cause):
                cloud = ProductionCloud()
                cloud.coordinator()
                cloud.service.watchdog()
                previous = copy.deepcopy(cloud.instances[0])
                cloud.instances = []
                def describe(**parameters):
                    if "InstanceIds" not in parameters:
                        return dict(Reservations=[])
                    if cause == "denied":
                        raise ApiError("AccessDenied", status=403)
                    if cause == "empty":
                        return dict(Reservations=[])
                    if cause == "paginated":
                        return dict(NextToken="unread-page", Reservations=[dict(
                            Instances=[dict(previous, State={"Name": "terminated"})])])
                    if cause in ("running", "wrong-id"):
                        row = copy.deepcopy(previous)
                        if cause == "wrong-id":
                            row.update(InstanceId="i-" + "7" * 17, State={"Name": "terminated"})
                        return dict(Reservations=[dict(Instances=[row])])
                    raise ApiError("InvalidInstanceID.NotFound", status=400)
                cloud.api["ec2"].describe_instances.side_effect = describe
                if cause in ("root-present", "root-denied"):
                    def volumes(**parameters):
                        if cause == "root-denied":
                            raise ApiError("UnauthorizedOperation", status=403)
                        return dict(Volumes=[dict(VolumeId=ROOT)])
                    cloud.api["ec2"].describe_volumes.side_effect = volumes
                elif cause == "missing-root":
                    state, revision = cloud.service.load()
                    state["seen"][INSTANCE]["root_volumes"] = []
                    cloud.service.save(state, revision)
                self.terminal_horizon(cloud)
                with self.assertRaises((ValueError, ApiError)):
                    cloud.service.watchdog()
                self.assertEqual(cloud.state["phase"], "closing")
                self.assertIsNone(cloud.state["cleanup"])
                self.assertEqual(cloud.state["seen"][INSTANCE]["state"], "running")

    def test_exact_terminal_identity_with_omitted_network_fields_recovers(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        terminal = copy.deepcopy(cloud.instances[0])
        terminal["State"]["Name"] = "terminated"
        for field in ("SubnetId", "IamInstanceProfile", "SecurityGroups"):
            del terminal[field]
        cloud.instances = []
        cloud.api["ec2"].describe_instances.side_effect = lambda **parameters: dict(
            Reservations=[dict(Instances=[terminal])] if "InstanceIds" in parameters else [])
        self.terminal_horizon(cloud)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "complete")
        self.assertEqual(cloud.state["cleanup"]["deleted_roots"], [ROOT])

    def test_received_identity_from_launching_state_survives_transient_policy_failure(self):
        cloud = self.uncertain(allocated=True)
        state, revision = cloud.service.load()
        # reproduce a process dying after the intent but before committing its unknown outcome.
        state["phase"] = "launching"
        item = cloud.database.items[cloud.owner.pk]
        item["state"]["S"] = json.dumps(state)
        def get(**parameters):
            if parameters["Key"]["pk"]["S"] == "policy":
                raise TimeoutError("policy unavailable")
            return cloud.production_get(**parameters)
        cloud.api["dynamodb"].get_item.side_effect = get
        self.assertEqual(cloud.service.watchdog()["state"], "deferred")
        self.assertEqual(cloud.state["launch_outcome"], "accepted")
        self.assertEqual(cloud.state["phase"], "requested")
        self.assertIsNone(cloud.state["binding"])
        self.assertNotIn(("terminate", [INSTANCE]), cloud.events)
        cloud.api["dynamodb"].get_item.side_effect = cloud.production_get
        cloud.service.watchdog()
        self.assertTrue(cloud.state["worker_published"])

    def test_receipt_revision_race_defers_without_writing_stale_bytes(self):
        cloud = ProductionCloud()
        old, revision = cloud.service.load()
        current = copy.deepcopy(old)
        current["next_attempt_at"] = common.timestamp(cloud.now + timedelta(seconds=1))
        cloud.service.save(current, revision)
        with self.assertRaises(common.RevisionConflict):
            cloud.service.receipt(old)
        cloud.api["s3"].put_object.assert_not_called()
        cloud.service.receipt(current)
        receipt = json.loads(cloud.api["s3"].put_object.call_args.kwargs["Body"])
        self.assertEqual(receipt["next_attempt_at"], current["next_attempt_at"])
        foreign = copy.deepcopy(old)
        foreign["request"]["controller"] = "f" * 40
        with self.assertRaises(ValueError):
            cloud.service.receipt(foreign)

    def test_first_terminal_record_can_omit_network_fields_but_cannot_bind(self):
        for physical in ("shutting-down", "terminated"):
            with self.subTest(physical=physical):
                cloud = self.uncertain(allocated=True)
                instance = cloud.instances[0]
                instance["State"]["Name"] = physical
                for field in ("SubnetId", "IamInstanceProfile", "SecurityGroups"):
                    del instance[field]
                cloud.service.watchdog()
                self.assertEqual(cloud.state["launch_outcome"], "accepted")
                self.assertEqual(cloud.state["phase"], "closing")
                self.assertIsNone(cloud.state["binding"])
                self.assertFalse(cloud.state["worker_published"])
                instance["State"]["Name"] = "terminated"
                cloud.service.watchdog()
                cloud.instances = []
                self.terminal_horizon(cloud)
                cloud.service.watchdog()
                self.assertEqual(cloud.state["phase"], "complete")
                self.assertEqual(cloud.state["cleanup"]["deleted_roots"], [ROOT])

    def test_terminal_present_mismatches_and_live_missing_fields_stay_unresolved(self):
        for physical in ("running", "terminated"):
            for field in ("SubnetId", "IamInstanceProfile", "SecurityGroups"):
                with self.subTest(physical=physical, field=field):
                    cloud = self.uncertain(allocated=True)
                    instance = cloud.instances[0]
                    instance["State"]["Name"] = physical
                    if physical == "running":
                        del instance[field]
                    else:
                        instance[field] = {"SubnetId": "wrong", "IamInstanceProfile": {"Arn": "wrong"},
                                           "SecurityGroups": [{"GroupId": "wrong"}]}[field]
                    cloud.service.watchdog()
                    self.assertEqual(cloud.state["launch_outcome"], "uncertain")
                    self.assertIsNone(cloud.state["binding"])
                    self.assertFalse(cloud.state["worker_published"])

    def output(self, cloud):
        paths = cloud.paths()
        archive = b"opaque archive retained for collector byte validation"
        status = json.dumps(dict(common.completed_status("123-1", True), sha256=common.digest(archive))).encode()
        for key, value in ((paths.status, status), (paths.results, archive)):
            cloud.objects[key], cloud.object_versions[key] = value, "original-" + common.digest(value)
            cloud.versions[(key, cloud.object_versions[key])] = value

    def test_completed_output_is_version_pinned_before_closure_and_current_replacement(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        self.output(cloud)
        cloud.service.watchdog()
        commitment = copy.deepcopy(cloud.state["worker_output"])
        self.assertEqual(commitment["results"]["sha256"], common.digest(cloud.objects[cloud.paths().results]))
        cloud.objects[cloud.paths().status] = b"foreign current object"
        cloud.objects[cloud.paths().results] = b"foreign archive"
        self.assertTrue(cloud.service.read_worker_status(cloud.state)["success"])
        cloud.instances[0]["State"]["Name"] = "terminated"
        self.terminal_horizon(cloud)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "complete")
        self.assertEqual(cloud.state["worker_output"], commitment)

    def test_closing_capture_retries_head_failure_without_skipping_termination(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        self.output(cloud)
        cloud.database.items["policy"]["enabled"] = {"BOOL": False}
        original = cloud.head_object
        def head(**parameters):
            if parameters["Key"] == cloud.paths().results:
                raise TimeoutError("archive HEAD unavailable")
            return original(**parameters)
        cloud.api["s3"].head_object.side_effect = head
        self.assertEqual(cloud.service.watchdog()["state"], "deferred")
        self.assertEqual(cloud.state["phase"], "closing")
        self.assertNotIn("worker_output", cloud.state)
        self.assertIn(("terminate", [INSTANCE]), cloud.events)
        cloud.api["s3"].head_object.side_effect = original
        cloud.instances[0]["State"]["Name"] = "terminated"
        cloud.service.watchdog()
        self.assertIn("worker_output", cloud.state)
        self.assertEqual(cloud.state["close_reason"], "production kill switch is off")
        self.terminal_horizon(cloud)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "complete")


if __name__ == "__main__":
    unittest.main()
