"""Physical reconciliation remains live through policy outages and lost launch replies."""

import base64
import copy
import json
from datetime import timedelta
import unittest
from unittest.mock import patch

from . import common, durable
from .test_durable_coordinator import ApiError, Cloud, INSTANCE, ROOT
from .test_production_runtime import ProductionCloud


class RuntimeReviewTests(unittest.TestCase):
    def setUp(self):
        network = patch("socket.create_connection", side_effect=AssertionError("offline tests only"))
        network.start()
        self.addCleanup(network.stop)

    def uncertain(self, kind=ProductionCloud):
        cloud = kind()
        cloud.launch_error, cloud.allocate_on_error = TimeoutError("lost reply"), True
        cloud.coordinator()
        self.assertEqual(cloud.state["launch_outcome"], "uncertain")
        return cloud

    def test_closing_observation_reconciles_identity_without_granting_runtime(self):
        for kind in (Cloud, ProductionCloud):
            for reason in ("launch acknowledgement", "kill switch", "capacity expiry", "guard failure"):
                with self.subTest(kind=kind.__name__, reason=reason):
                    cloud = self.uncertain(kind)
                    allocation = copy.deepcopy(cloud.state["attempt"])
                    state, revision = cloud.service.load()
                    state.update(phase="closing", close_reason=reason)
                    cloud.service.save(state, revision)
                    cloud.service.watchdog()
                    self.assertEqual(cloud.state["launch_outcome"], "accepted")
                    self.assertEqual(cloud.state["close_reason"], reason)
                    self.assertIsNone(cloud.state["binding"])
                    self.assertFalse(cloud.state["runtime_verified"])
                    self.assertFalse(cloud.state["worker_published"])
                    cloud.api["scheduler"].create_schedule.assert_not_called()
                    self.assertEqual(cloud.state["attempt"], allocation)
                    self.assertIn(("terminate", [INSTANCE]), cloud.events)
                    cloud.instances[0]["State"]["Name"] = "terminated"
                    cloud.service.watchdog()
                    self.assertEqual(cloud.state["seen"][INSTANCE]["state"], "terminated")
                    cloud.instances = []
                    cloud.now = common.utc(cloud.request["outer_deadline"]) + timedelta(
                        seconds=durable.DELIVERY_DRAIN_SECONDS)
                    cloud.service.watchdog()
                    self.assertEqual(cloud.state["phase"], "complete")
                    self.assertEqual(cloud.state["cleanup"]["deleted_roots"], [ROOT])
                    self.assertFalse(cloud.state["cleanup"]["unresolved_allocation"])
                    if kind is ProductionCloud:
                        receipt = cloud.api["s3"].put_object.call_args.kwargs
                        self.assertEqual(receipt["ChecksumSHA256"],
                            base64.b64encode(bytes.fromhex(common.digest(receipt["Body"]))).decode("ascii"))
                    before = len(cloud.launches)
                    cloud.coordinator()
                    self.assertEqual(len(cloud.launches), before)

    def test_late_inventory_recovers_after_ack_closure_and_capacity_expiry(self):
        for delay in (durable.RUNTIME_SECONDS, durable.CAPACITY_SECONDS + 1):
            with self.subTest(delay=delay):
                cloud = self.uncertain()
                instances, cloud.instances = cloud.instances, []
                cloud.now += timedelta(seconds=delay)
                cloud.service.watchdog()
                self.assertEqual(cloud.state["phase"], "closing")
                reason = cloud.state["close_reason"]
                cloud.instances = instances
                cloud.service.watchdog()
                self.assertEqual(cloud.state["launch_outcome"], "accepted")
                self.assertEqual(cloud.state["close_reason"], reason)
                self.assertIsNone(cloud.state["binding"])
                self.assertFalse(cloud.state["worker_published"])

    def test_matching_allocation_launched_after_capacity_deadline_is_evidence_not_runtime(self):
        cloud = self.uncertain()
        cloud.now = common.utc(cloud.request["capacity_deadline"]) + timedelta(seconds=1)
        cloud.instances[0]["LaunchTime"] = cloud.now
        cloud.service.watchdog()
        self.assertEqual(cloud.state["launch_outcome"], "accepted")
        self.assertEqual(cloud.state["phase"], "closing")
        self.assertIsNone(cloud.state["binding"])
        self.assertFalse(cloud.state["worker_published"])

    def test_foreign_observation_cannot_resolve_an_unknown_allocation(self):
        variants = dict(token=("ClientToken", "foreign"), image=("ImageId", "ami-" + "9" * 17),
            profile=("IamInstanceProfile", {"Arn": "foreign"}), subnet=("SubnetId", "foreign"),
            group=("SecurityGroups", [{"GroupId": "foreign"}]), purchase=("InstanceLifecycle", "spot"))
        for label, (key, value) in variants.items():
            with self.subTest(label=label):
                cloud = self.uncertain()
                cloud.instances[0][key] = value
                state, revision = cloud.service.load()
                state.update(phase="closing", close_reason="close before identity")
                cloud.service.save(state, revision)
                cloud.service.watchdog()
                self.assertEqual(cloud.state["launch_outcome"], "uncertain")
                self.assertIsNone(cloud.state["binding"])
                cloud.instances[0]["State"]["Name"] = "terminated"
                cloud.service.watchdog()
                cloud.instances = []
                cloud.now = common.utc(cloud.request["outer_deadline"]) + timedelta(
                    seconds=durable.DELIVERY_DRAIN_SECONDS)
                with self.assertRaisesRegex(ValueError, "identity has not reconciled"):
                    cloud.service.watchdog()
                self.assertEqual(cloud.state["phase"], "closing")
                self.assertIsNone(cloud.state["cleanup"])

    def test_persistent_policy_failure_keeps_terminal_observations_through_ec2_expiry(self):
        for failure in ("missing", "malformed", "denied"):
            with self.subTest(failure=failure):
                cloud = self.uncertain()
                def get(**parameters):
                    if parameters["Key"]["pk"]["S"] == "policy":
                        if failure == "denied":
                            raise ApiError("AccessDenied", status=403)
                        return {} if failure == "missing" else {"Item": {"enabled": {"S": "true"}}}
                    return cloud.production_get(**parameters)
                cloud.api["dynamodb"].get_item.side_effect = get
                for physical in ("running", "shutting-down", "terminated"):
                    cloud.instances[0]["State"]["Name"] = physical
                    with self.assertRaises((ApiError, ValueError)):
                        cloud.service.watchdog()
                    self.assertEqual(cloud.state["seen"][INSTANCE]["state"], physical)
                    self.assertEqual(cloud.state["launch_outcome"], "accepted")
                    self.assertFalse(cloud.state["worker_published"])
                    self.assertIsNone(cloud.state["binding"])
                cloud.instances = []
                cloud.now = common.utc(cloud.request["outer_deadline"]) + timedelta(
                    seconds=durable.DELIVERY_DRAIN_SECONDS)
                with self.assertRaises((ApiError, ValueError)):
                    cloud.service.watchdog()
                self.assertEqual(cloud.state["phase"], "complete")
                self.assertEqual(cloud.state["cleanup"]["deleted_roots"], [ROOT])
                self.assertEqual(cloud.service.watchdog()["state"], "complete")

    def test_lost_production_intent_and_readback_reconcile_not_sent_before_another_launch(self):
        cloud = ProductionCloud()
        write, read = cloud.database.transact_write_items, cloud.database.transact_get_items
        pending = {"lose": True, "readback": False}
        def transact_write(**parameters):
            state = next((json.loads(item["Update"]["ExpressionAttributeValues"][":state"]["S"])
                          for item in parameters["TransactItems"] if "Update" in item and
                          ":state" in item["Update"]["ExpressionAttributeValues"]), None)
            result = write(**parameters)
            if state and state["phase"] == "launching" and pending["lose"]:
                pending.update(lose=False, readback=True)
                raise TimeoutError("launch intent committed but response lost")
            return result
        def transact_read(**parameters):
            if pending["readback"]:
                pending["readback"] = False
                raise TimeoutError("readback unavailable")
            return read(**parameters)
        cloud.api["dynamodb"].transact_write_items.side_effect = transact_write
        cloud.api["dynamodb"].transact_get_items.side_effect = transact_read
        with self.assertRaises(TimeoutError):
            cloud.coordinator()
        self.assertEqual(cloud.state["launch_outcome"], "not-sent")
        self.assertFalse(cloud.state["allocation_consumed"])
        self.assertEqual(cloud.launches, [])
        self.assertEqual(cloud.state["attempts"], 1)
        cloud.now += timedelta(seconds=durable.RETRY_SECONDS)
        cloud.coordinator()
        self.assertEqual(len(cloud.launches), 1)
        self.assertEqual(cloud.state["attempts"], 2)
        self.assertEqual(cloud.state["launch_outcome"], "accepted")

    def test_capacity_proof_retries_transient_transaction_without_losing_negative_evidence(self):
        cloud = ProductionCloud()
        write, attempts = cloud.database.transact_write_items, []
        def transact_write(**parameters):
            state = next((json.loads(item["Update"]["ExpressionAttributeValues"][":state"]["S"])
                          for item in parameters["TransactItems"] if "Update" in item and
                          ":state" in item["Update"]["ExpressionAttributeValues"]), None)
            if state and state["launch_outcome"] == "capacity-rejected":
                attempts.append(copy.deepcopy(parameters))
                if len(attempts) < 3:
                    error = ApiError("TransactionCanceledException", status=400)
                    error.response["CancellationReasons"] = [{"Code": "None"}, {"Code": "TransactionConflict"}]
                    raise error
            return write(**parameters)
        cloud.api["dynamodb"].transact_write_items.side_effect = transact_write
        cloud.launch_error = ApiError("InsufficientInstanceCapacity", status=400)
        cloud.coordinator()
        self.assertEqual(cloud.state["launch_outcome"], "capacity-rejected")
        self.assertFalse(cloud.state["allocation_consumed"])
        self.assertEqual(len(cloud.state["capacity_failures"]), 1)
        self.assertEqual(len(cloud.launches), 1)
        self.assertEqual(cloud.sleeps, [1, 2])
        self.assertEqual(len(attempts), 3)
        self.assertTrue(all(attempt["TransactItems"] == attempts[0]["TransactItems"] for attempt in attempts))

    def test_transient_policy_failure_records_identity_without_starting_useful_work(self):
        cloud = self.uncertain()
        def get(**parameters):
            if parameters["Key"]["pk"]["S"] == "policy":
                raise TimeoutError("policy response lost")
            return cloud.production_get(**parameters)
        cloud.api["dynamodb"].get_item.side_effect = get
        self.assertEqual(cloud.service.watchdog()["state"], "deferred")
        self.assertEqual(cloud.state["launch_outcome"], "accepted")
        self.assertIsNone(cloud.state["binding"])
        self.assertFalse(cloud.state["worker_published"])
        cloud.api["scheduler"].create_schedule.assert_not_called()
        cloud.api["dynamodb"].get_item.side_effect = cloud.production_get
        cloud.service.watchdog()
        self.assertTrue(cloud.state["worker_published"])


if __name__ == "__main__":
    unittest.main()
