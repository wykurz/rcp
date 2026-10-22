"""Offline failure ordering, bounded retry, and historical receipt regressions."""

import copy
from dataclasses import replace
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

from infra.aws_benchmark import common, durable, durable_aws
from infra.aws_benchmark.test_durable_coordinator import ApiError, Cloud, INSTANCE, ROOT, ProcessDeath


class MissingResponseTimeout(TimeoutError):
    response = None


class LaunchRecoveryTests(unittest.TestCase):
    def test_transient_launch_retries_identical_intent_and_recovers_acceptance(self):
        for error in (ApiError("RequestLimitExceeded", status=400),
                      ApiError("Unavailable", status=503), MissingResponseTimeout()):
            with self.subTest(error=type(error).__name__):
                c = Cloud()
                calls = []
                def launch(**kw):
                    calls.append(copy.deepcopy(kw))
                    if len(calls) == 1:
                        raise error
                    return c.run_instances(**kw)
                c.api["ec2"].run_instances.side_effect = launch
                c.coordinator()
                self.assertEqual(calls, [c.state["attempt"]["parameters"]] * 2)
                self.assertEqual(c.sleeps, [1])
                self.assertEqual(c.state["attempts"], 1)
                self.assertEqual(c.state["launch_outcome"], "accepted")
                self.assertEqual(c.state["known_instance_ids"], [INSTANCE])

    def test_negative_response_after_uncertainty_never_releases_allocation(self):
        c = Cloud()
        c.api["ec2"].run_instances.side_effect = [
            MissingResponseTimeout(), ApiError("InsufficientInstanceCapacity", status=400)]
        c.coordinator()
        self.assertEqual(c.state["launch_outcome"], "uncertain")
        self.assertTrue(c.state["allocation_consumed"])
        self.assertEqual(c.state["capacity_failures"], [])
        before = copy.deepcopy(c.state["attempt"])
        c.now += timedelta(minutes=10)
        c.coordinator()
        self.assertEqual(c.api["ec2"].run_instances.call_count, 2)
        self.assertEqual(c.state["attempt"], before)

    def test_retries_stop_at_capacity_deadline_stale_guard_or_invocation_budget(self):
        for limit in ("capacity", "guard", "invocation"):
            with self.subTest(limit=limit):
                c = Cloud()
                def launch(**kw):
                    c.launches.append(copy.deepcopy(kw))
                    if limit == "capacity":
                        c.now = common.utc(c.request["capacity_deadline"])
                    elif limit == "guard":
                        c.now += timedelta(seconds=durable.GUARD_GAP_SECONDS + 1)
                    else:
                        c.service.retry_until = c.service.monotonic()
                    raise MissingResponseTimeout()
                c.api["ec2"].run_instances.side_effect = launch
                c.coordinator()
                self.assertEqual(len(c.launches), 1)
                self.assertEqual(c.sleeps, [])
                self.assertTrue(c.state["allocation_consumed"])
                self.assertEqual(c.state["launch_outcome"], "uncertain")

    def test_deadline_crossing_during_backoff_cannot_send_retry(self):
        c = Cloud()
        c.launch_error = MissingResponseTimeout()
        def sleep(delay):
            c.sleeps.append(delay)
            c.now = common.utc(c.request["capacity_deadline"])
        c.service.sleep = sleep
        c.coordinator()
        self.assertEqual(len(c.launches), 1)
        self.assertEqual(c.sleeps, [1])
        self.assertTrue(c.state["allocation_consumed"])

    def test_proof_reconciliation_backs_off_after_brownout_for_each_outcome(self):
        for mode in ("accepted", "capacity-rejected", "not-sent"):
            with self.subTest(mode=mode):
                c = Cloud()
                calls = []
                def write(**kw):
                    state = json.loads(kw["ExpressionAttributeValues"][":payload"]["S"])
                    if state["launch_outcome"] == mode:
                        calls.append(kw)
                        if len(calls) < 3:
                            raise ApiError("ProvisionedThroughputExceededException", status=400)
                    if mode == "not-sent" and state["phase"] == "launching":
                        c.update_item(**kw)
                        raise MissingResponseTimeout()
                    return c.update_item(**kw)
                c.api["dynamodb"].update_item.side_effect = write
                if mode == "capacity-rejected":
                    c.launch_error = ApiError("InsufficientInstanceCapacity", status=400)
                c.handler("coordinator")
                self.assertEqual(c.state["launch_outcome"], mode)
                self.assertEqual(c.sleeps, [1, 2])
                self.assertEqual(len(calls), 3)
                self.assertEqual(c.state["allocation_consumed"], mode == "accepted")
                self.assertEqual(len(c.state["capacity_failures"]), int(mode == "capacity-rejected"))
                self.assertEqual(len(c.launches), int(mode != "not-sent"))

    def test_reconciliation_requires_one_evidence_mode(self):
        c = Cloud()
        allocation = durable.attempt(c.request, 1, c.now)
        for modes in ({}, dict(response={}, failure=TimeoutError()), dict(not_sent=True, response={})):
            with self.subTest(modes=modes), self.assertRaisesRegex(ValueError, "exactly one"):
                c.service.finish_launch(c.request["run"], allocation, **modes)
        c.api["dynamodb"].update_item.assert_not_called()

    def test_process_timeout_reentry_after_intent_cannot_allocate_with_empty_inventory(self):
        c = Cloud()
        c.launch_error = ProcessDeath()
        with self.assertRaises(ProcessDeath):
            c.coordinator()
        intent = copy.deepcopy(c.state["attempt"])
        c.launch_error = None
        c.coordinator()
        c.now += timedelta(seconds=durable.LAUNCH_ACK_SECONDS + 1)
        c.service.watchdog()
        c.coordinator()
        self.assertEqual(c.instances, [])
        self.assertEqual(len(c.launches), 1)
        self.assertEqual(c.state["attempt"], intent)
        self.assertTrue(c.state["allocation_consumed"])

    def test_process_timeout_before_intent_allows_safe_first_launch_on_reentry(self):
        c = Cloud()
        c.api["s3"].head_object.side_effect = ProcessDeath()
        with self.assertRaises(ProcessDeath):
            c.coordinator()
        self.assertEqual(c.launches, [])
        self.assertFalse(c.state["allocation_consumed"])
        c.api["s3"].head_object.side_effect = c.head_object
        c.coordinator()
        self.assertEqual(len(c.launches), 1)
        self.assertEqual(c.state["attempts"], 1)

    def test_expiry_preserves_existing_reason_without_allocating(self):
        c = Cloud()
        c.state["close_reason"] = "operator close"
        c.now = common.utc(c.request["capacity_deadline"])
        c.coordinator()
        self.assertEqual(c.state["close_reason"], "operator close")
        self.assertEqual(c.state["phase"], "closing")
        self.assertEqual(c.launches, [])


class WatchdogRecoveryTests(unittest.TestCase):
    def invalid_schedule(self):
        c = Cloud()
        c.coordinator()
        binding = durable.bind(c.request, c.instances[0], c.state["attempt"])
        schedule = c.service.schedule(binding=binding)
        schedule["ScheduleExpression"] = "at(2099-01-01T00:00:00)"
        c.schedules[(schedule["GroupName"], schedule["Name"])] = schedule
        return c

    def test_contract_error_survives_save_and_termination_failures_and_receipt_is_attempted(self):
        for fail_save, fail_terminate in ((True, False), (False, True), (True, True)):
            with self.subTest(save=fail_save, terminate=fail_terminate):
                c = self.invalid_schedule()
                def write(**kw):
                    state = json.loads(kw["ExpressionAttributeValues"][":payload"]["S"])
                    if fail_save and state["phase"] == "closing":
                        raise ApiError("ProvisionedThroughputExceededException", status=400)
                    return c.update_item(**kw)
                c.api["dynamodb"].update_item.side_effect = write
                if fail_terminate:
                    c.api["ec2"].terminate_instances.side_effect = MissingResponseTimeout()
                with self.assertRaisesRegex(ValueError, "cutoff metadata differs"):
                    c.service.watchdog()
                c.api["ec2"].terminate_instances.assert_called_once_with(InstanceIds=[INSTANCE])
                self.assertIn(durable_aws.RECEIPT, c.objects)
                self.assertIsNone(json.loads(c.objects[durable_aws.RECEIPT])["cleanup"])
                self.assertFalse(c.state["worker_published"])
                if not fail_save:
                    self.assertEqual(c.state["phase"], "closing")
                    self.assertIn("guard failure", c.state["close_reason"])

    def test_observed_close_terminates_before_state_save_even_when_save_fails(self):
        c = Cloud()
        c.coordinator()
        c.instances[0]["ClientToken"] = "wrong"
        c.api["dynamodb"].update_item.side_effect = ApiError("ConditionalCheckFailedException", status=400)
        self.assertEqual(c.service.watchdog()["state"], "deferred")
        c.api["ec2"].terminate_instances.assert_called_once_with(InstanceIds=[INSTANCE])
        self.assertIsNone(json.loads(c.objects[durable_aws.RECEIPT])["cleanup"])

    def test_lost_publication_save_can_replay_after_initial_admission_margin(self):
        c = Cloud()
        c.coordinator()
        def write(**kw):
            state = json.loads(kw["ExpressionAttributeValues"][":payload"]["S"])
            if state["worker_published"]:
                raise MissingResponseTimeout()
            return c.update_item(**kw)
        c.api["dynamodb"].update_item.side_effect = write
        self.assertEqual(c.service.watchdog()["state"], "deferred")
        self.assertIn("publication", c.state)
        self.assertFalse(c.state["worker_published"])
        first_window = c.objects[common.WINDOW_KEY]
        deadline = common.utc(c.state["binding"]["deadline"])
        c.now = deadline - timedelta(seconds=durable.worker_minimum(c.request["plan"]) - 1)
        c.api["dynamodb"].update_item.side_effect = c.update_item
        self.assertEqual(c.service.watchdog()["state"], "requested")
        self.assertTrue(c.state["worker_published"])
        self.assertEqual(c.objects[common.WINDOW_KEY], first_window)
        self.assertEqual(common.utc(c.state["binding"]["deadline"]), deadline)
        c.api["ec2"].terminate_instances.assert_not_called()

    def test_unpersisted_publication_intent_never_sends_work(self):
        c = Cloud()
        c.coordinator()
        def write(**kw):
            state = json.loads(kw["ExpressionAttributeValues"][":payload"]["S"])
            if "publication" in state:
                raise MissingResponseTimeout()
            return c.update_item(**kw)
        c.api["dynamodb"].update_item.side_effect = write
        self.assertEqual(c.service.watchdog()["state"], "deferred")
        self.assertNotIn(common.WINDOW_KEY, c.objects)
        self.assertNotIn(common.REQUEST_KEY, c.objects)
        self.assertNotIn("publication", c.state)

    def test_new_publication_still_requires_full_time_and_saved_binding(self):
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt):
                c = Cloud()
                c.coordinator()
                if corrupt:
                    c.state["publication"] = dict(attempt_id="wrong", instance_id=INSTANCE, deadline="wrong")
                else:
                    c.now += timedelta(seconds=c.request["runtime_seconds"] - durable.worker_minimum(c.request["plan"]) + 1)
                with self.assertRaises(ValueError):
                    c.service.watchdog()
                self.assertNotIn(common.WINDOW_KEY, c.objects)
                c.api["ec2"].terminate_instances.assert_called()

    def test_response_none_transient_status_error_defers_healthy_worker(self):
        c = Cloud()
        c.coordinator()
        c.service.watchdog()
        c.api["s3"].get_object.side_effect = MissingResponseTimeout()
        self.assertEqual(c.service.watchdog()["state"], "deferred")
        self.assertTrue(c.state["worker_published"])
        self.assertIsNone(c.state["close_reason"])
        c.api["ec2"].terminate_instances.assert_not_called()

    def pending_without_root(self):
        c = Cloud()
        def launch(**kw):
            result = c.run_instances(**kw)
            for instance in (result["Instances"][0], c.instances[0]):
                instance["State"]["Name"] = "pending"
                instance["BlockDeviceMappings"] = []
            return result
        c.api["ec2"].run_instances.side_effect = launch
        c.coordinator()
        return c

    def test_closing_termination_precedes_failing_tag_discovery_without_binding(self):
        c = self.pending_without_root()
        c.state.update(phase="closing", close_reason="explicit close")
        def roots(**kw):
            self.assertIn(("terminate", [INSTANCE]), c.events)
            raise ApiError("ThrottlingException", status=400)
        c.api["ec2"].describe_volumes.side_effect = roots
        self.assertEqual(c.service.watchdog()["state"], "deferred")
        self.assertIsNone(c.state["binding"])
        self.assertIsNone(c.state["cleanup"])
        self.assertEqual(c.state["close_reason"], "explicit close")

    def test_terminated_instance_missing_launch_fields_can_recover_tagged_root(self):
        c = self.pending_without_root()
        c.instances[0]["State"]["Name"] = "terminated"
        for key in ("SubnetId", "SecurityGroups", "IamInstanceProfile"):
            del c.instances[0][key]
        volume = dict(VolumeId=ROOT, Tags=c.instances[0]["Tags"], Attachments=[])
        c.api["ec2"].describe_volumes.side_effect = lambda **kw: {"Volumes": [volume]}
        c.service.watchdog()
        self.assertIsNone(c.state["binding"])
        self.assertEqual(c.state["seen"][INSTANCE]["root_volumes"], [ROOT])
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        c.api["ec2"].describe_volumes.side_effect = c.describe_volumes
        c.service.watchdog()
        self.assertEqual(c.state["cleanup"]["deleted_roots"], [ROOT])
        self.assertEqual(c.state["phase"], "complete")

    def test_failed_terminal_save_cannot_publish_uncommitted_cleanup(self):
        c = Cloud()
        c.coordinator()
        c.service.watchdog()
        c.instances[0]["State"]["Name"] = "terminated"
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        c.api["dynamodb"].update_item.side_effect = MissingResponseTimeout()
        self.assertEqual(c.service.watchdog()["state"], "deferred")
        self.assertIsNone(c.state["cleanup"])
        self.assertIsNone(json.loads(c.objects[durable_aws.RECEIPT])["cleanup"])
        c.api["dynamodb"].update_item.side_effect = c.update_item
        c.service.watchdog()
        self.assertIsNotNone(json.loads(c.objects[durable_aws.RECEIPT])["cleanup"])


class HistoricalLeaseTests(unittest.TestCase):
    def complete(self):
        c = Cloud()
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        c.service.watchdog()
        self.assertEqual(c.state["phase"], "complete")
        return c

    def test_completed_receipt_repair_survives_current_plan_and_deployment_pin_upgrade(self):
        c = self.complete()
        expected, revision = c.objects[durable_aws.RECEIPT], c.revision
        c.service.config = replace(c.config, source="f" * 40, ami="ami-" + "f" * 17, lt_version="10")
        with patch.object(common, "DURABLE_PLAN", {**common.DURABLE_PLAN, "version": "future-plan"}):
            c.now += timedelta(days=1)
            self.assertEqual(c.service.watchdog()["state"], "complete")
        self.assertEqual(c.objects[durable_aws.RECEIPT], expected)
        self.assertEqual(c.revision, revision)

    def test_completed_receipt_requires_original_namespace_and_valid_recorded_request(self):
        for key, value in (("account", "999999999999"), ("region", "eu-west-1"),
                           ("namespace", "another"), ("bucket", "other-bucket")):
            with self.subTest(key=key):
                c = self.complete()
                c.service.config = replace(c.config, **{key: value})
                with self.assertRaisesRegex(ValueError, "namespace changed"):
                    c.service.load()
        c = self.complete()
        c.state["request"]["plan"]["version"] = "unregistered"
        with self.assertRaises(ValueError):
            c.service.load()

    def test_unresolved_lease_still_requires_current_plan_and_all_deployment_pins(self):
        c = Cloud()
        c.service.config = replace(c.config, lt_version="10")
        with self.assertRaisesRegex(ValueError, "deployment pins changed"):
            c.service.load()
        c.service.config = c.config
        with patch.object(common, "DURABLE_PLAN", {**common.DURABLE_PLAN, "version": "future-plan"}):
            with self.assertRaisesRegex(ValueError, "not current"):
                c.service.load()


if __name__ == "__main__":
    unittest.main()
