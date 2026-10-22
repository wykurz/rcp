"""Offline regressions for monotonic launch evidence and recoverable transient failures."""

import copy
from datetime import timedelta
import json
import unittest

from infra.aws_benchmark import common, durable, durable_aws
from infra.aws_benchmark.test_durable_coordinator import Cloud, ApiError, INSTANCE, ROOT


class RecoveryTests(unittest.TestCase):
    def test_transient_coordinator_reads_defer_before_and_after_allocation(self):
        for launched in (False, True):
            for code in ("ThrottlingException", "ProvisionedThroughputExceededException"):
                with self.subTest(launched=launched, code=code):
                    c = Cloud()
                    if launched:
                        c.coordinator()
                    before = copy.deepcopy(c.state)
                    c.api["dynamodb"].get_item.side_effect = ApiError(code, status=400)
                    self.assertEqual(c.handler("coordinator")["state"], "wait")
                    self.assertEqual(c.state, before)
                    self.assertEqual(len(c.launches), int(launched))

    def test_real_sdk_timeout_is_classified_as_uncertain_then_recovered_by_positive_evidence(self):
        try:
            from botocore.exceptions import ReadTimeoutError
        except ImportError:
            self.skipTest("pinned AWS SDK is not installed")
        c = Cloud()
        c.launch_error = ReadTimeoutError(endpoint_url="https://offline.invalid")
        c.allocate_on_error = True
        self.assertIsNone(durable_aws.error_code(c.launch_error))
        self.assertFalse(durable_aws.capacity_rejection(c.launch_error))
        c.coordinator()
        self.assertEqual(c.state["launch_outcome"], "uncertain")
        deadline = c.state["attempt"]["close_at"]
        c.service.watchdog()
        self.assertEqual(c.state["launch_outcome"], "accepted")
        self.assertEqual(c.state["binding"]["close_at"], deadline)
        self.assertTrue(c.state["worker_published"])
        self.assertEqual(len(c.launches), 3)
        self.assertTrue(all(parameters == c.launches[0] for parameters in c.launches))

    def test_known_acceptance_survives_transient_write_before_or_after_commit(self):
        for committed in (False, True):
            with self.subTest(committed=committed):
                c = Cloud()
                def write(**kw):
                    candidate = json.loads(kw["ExpressionAttributeValues"][":payload"]["S"])
                    if candidate["launch_outcome"] == "accepted":
                        c.api["dynamodb"].update_item.side_effect = c.update_item
                        if committed:
                            c.update_item(**kw)
                        raise ApiError("ProvisionedThroughputExceededException", status=400)
                    return c.update_item(**kw)
                c.api["dynamodb"].update_item.side_effect = write
                c.coordinator()
                self.assertEqual(c.state["known_instance_ids"], [INSTANCE])
                self.assertEqual(c.state["seen"][INSTANCE]["root_volumes"], [ROOT])
                self.assertEqual(c.state["launch_outcome"], "accepted")
                c.service.finish_launch(c.request["run"], c.state["attempt"], failure=TimeoutError())
                self.assertEqual(c.state["launch_outcome"], "accepted")
                self.assertEqual(len(c.launches), 1)

    def test_exhausted_acceptance_write_recovers_without_a_second_launch(self):
        c = Cloud()
        def write(**kw):
            candidate = json.loads(kw["ExpressionAttributeValues"][":payload"]["S"])
            if candidate["launch_outcome"] == "accepted":
                raise ApiError("ThrottlingException", status=400)
            return c.update_item(**kw)
        c.api["dynamodb"].update_item.side_effect = write
        self.assertEqual(c.handler("coordinator")["state"], "wait")
        self.assertEqual(c.state["phase"], "launching")
        c.api["dynamodb"].update_item.side_effect = c.update_item
        c.service.watchdog()
        self.assertEqual(c.state["known_instance_ids"], [INSTANCE])
        self.assertTrue(c.state["worker_published"])
        c.coordinator()
        self.assertEqual(len(c.launches), 1)

    def test_capacity_rejection_after_concurrent_close_retains_no_allocation_proof(self):
        c = Cloud()
        def launch(**kw):
            c.now = common.utc(c.request["capacity_deadline"])
            c.service.watchdog()
            raise ApiError("InsufficientInstanceCapacity", status=400)
        c.api["ec2"].run_instances.side_effect = launch
        c.coordinator()
        self.assertEqual(c.state["phase"], "closing")
        self.assertFalse(c.state["allocation_consumed"])
        self.assertEqual(c.state["capacity_failures"][0]["request_id"], "request-123")
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        c.service.watchdog()
        self.assertEqual(c.state["phase"], "complete")

    def test_unknown_intent_write_is_reconciled_as_not_sent(self):
        for committed in (False, True):
            with self.subTest(committed=committed):
                c = Cloud()
                def write(**kw):
                    c.api["dynamodb"].update_item.side_effect = c.update_item
                    if committed:
                        c.update_item(**kw)
                    raise TimeoutError("write result lost")
                c.api["dynamodb"].update_item.side_effect = write
                self.assertEqual(c.handler("coordinator")["state"], "wait")
                self.assertFalse(c.state["allocation_consumed"])
                self.assertEqual(c.launches, [])
                self.assertEqual(c.state["launch_outcome"], "not-sent" if committed else "none")

    def test_capacity_rejection_retry_does_not_duplicate_evidence(self):
        c = Cloud()
        def write(**kw):
            value = json.loads(kw["ExpressionAttributeValues"][":payload"]["S"])
            c.update_item(**kw)
            if value["launch_outcome"] == "capacity-rejected":
                c.api["dynamodb"].update_item.side_effect = c.update_item
                raise TimeoutError("committed rejection result lost")
        c.api["dynamodb"].update_item.side_effect = write
        c.launch_error = ApiError("InsufficientInstanceCapacity", status=400)
        c.coordinator()
        self.assertEqual(len(c.state["capacity_failures"]), 1)
        self.assertFalse(c.state["allocation_consumed"])

    def test_nonmatching_multiple_future_or_late_observations_never_recover_acceptance(self):
        for kind in ("token", "tags", "image", "future", "multiple", "already-closing"):
            with self.subTest(kind=kind):
                c = Cloud()
                c.launch_error, c.allocate_on_error = TimeoutError(), True
                c.coordinator()
                if kind == "token":
                    c.instances[0]["ClientToken"] = "wrong"
                elif kind == "tags":
                    c.instances[0]["Tags"] = [v for v in c.instances[0]["Tags"] if v["Key"] != durable.TAG_ATTEMPT]
                elif kind == "image":
                    c.instances[0]["ImageId"] = "ami-" + "9" * 17
                elif kind == "future":
                    c.instances[0]["LaunchTime"] += timedelta(seconds=1)
                elif kind == "multiple":
                    c.instances.append(c.instance("i-" + "9" * 17))
                else:
                    c.state.update(phase="closing", close_reason="explicit prior close")
                c.service.watchdog()
                self.assertEqual(c.state["phase"], "closing")
                self.assertFalse(c.state["worker_published"])
                self.assertIsNone(c.state["binding"])
                self.assertTrue(any(event[0] == "terminate" for event in c.events))
                self.assertEqual(len(c.launches), 3)
                self.assertTrue(all(parameters == c.launches[0] for parameters in c.launches))

    def test_ambiguous_absent_instance_keeps_hold_after_drain(self):
        c = Cloud()
        c.launch_error = TimeoutError()
        c.coordinator()
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(hours=1)
        with self.assertRaisesRegex(ValueError, "empty Describe"):
            c.service.watchdog()
        self.assertTrue(c.state["allocation_consumed"])
        self.assertIsNone(c.state["cleanup"])

    def test_transient_status_read_keeps_healthy_worker_and_does_not_recheck_schedule(self):
        c = Cloud()
        c.coordinator()
        c.service.watchdog()
        revision = c.revision
        c.events.clear()
        c.api["scheduler"].get_schedule.reset_mock()
        c.api["scheduler"].get_schedule.side_effect = ApiError("ThrottlingException", status=400)
        c.api["s3"].get_object.side_effect = ApiError("SlowDown", status=503)
        self.assertEqual(c.service.watchdog()["state"], "deferred")
        self.assertEqual(c.state["phase"], "requested")
        self.assertIsNone(c.state["close_reason"])
        self.assertEqual(c.revision, revision)
        c.api["scheduler"].get_schedule.assert_not_called()
        self.assertFalse(any(e[0] == "terminate" for e in c.events))

    def test_transient_first_schedule_read_defers_without_publishing(self):
        c = Cloud()
        c.coordinator()
        c.api["scheduler"].get_schedule.side_effect = ApiError("ThrottlingException", status=400)
        self.assertEqual(c.service.watchdog()["state"], "deferred")
        self.assertFalse(c.state["worker_published"])
        self.assertIsNone(c.state["close_reason"])
        self.assertFalse(any(e[0] == "terminate" for e in c.events))

    def test_capacity_expiry_during_artifact_checks_closes_without_an_intent(self):
        c = Cloud()
        c.get_hook = lambda: setattr(c, "now", common.utc(c.request["capacity_deadline"]))
        self.assertEqual(c.coordinator()["state"], "wait")
        self.assertEqual(c.state["phase"], "closing")
        self.assertEqual(c.launches, [])
        self.assertFalse(c.state["allocation_consumed"])

    def test_checksum_heads_replace_downloads_and_reject_invalid_checksum_or_size(self):
        c = Cloud()
        c.coordinator()
        c.api["s3"].get_object.assert_not_called()
        self.assertEqual(c.api["s3"].head_object.call_count, len(common.BINARIES))
        self.assertTrue(all(call.kwargs["ChecksumMode"] == "ENABLED" for call in c.api["s3"].head_object.call_args_list))
        for change in ({"ChecksumSHA256": None}, {"ChecksumType": "COMPOSITE"},
                       {"ContentLength": 0}, {"ContentLength": common.MAX_BINARY + 1}):
            with self.subTest(change=change):
                c = Cloud()
                c.api["s3"].head_object.side_effect = lambda **kw: dict(c.head_object(**kw), **change)
                with self.assertRaisesRegex(ValueError, "digest or size"):
                    c.coordinator()
                self.assertEqual(c.launches, [])

    def test_idle_watchdog_does_not_churn_lease_revision(self):
        c = Cloud()
        c.service.watchdog()
        c.now += timedelta(minutes=1)
        c.service.watchdog()
        self.assertEqual(c.revision, 0)
        self.assertEqual(c.state["attempts"], 0)

    def test_terminal_cleanup_time_and_phase_survive_later_io_failure(self):
        c = Cloud()
        c.coordinator()
        c.service.watchdog()
        c.instances[0]["State"]["Name"] = "terminated"
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        c.service.watchdog()
        proof, revision = copy.deepcopy(c.state), c.revision
        receipt = c.objects[durable_aws.RECEIPT]
        c.now += timedelta(minutes=1)
        c.api["ec2"].describe_volumes.side_effect = ApiError("ThrottlingException", status=400)
        c.api["s3"].put_object.side_effect = TimeoutError("receipt response lost")
        self.assertEqual(c.handler("watchdog")["state"], "deferred")
        self.assertEqual(c.state, proof)
        c.api["s3"].put_object.side_effect = c.put_object
        c.service.watchdog()
        self.assertEqual(json.loads(c.objects[durable_aws.RECEIPT]), json.loads(receipt))
        self.assertEqual(c.revision, revision)

    def test_accepted_pending_termination_never_authorizes_replacement(self):
        c = Cloud()
        c.coordinator()
        c.instances[0]["State"]["Name"] = "terminated"
        c.instances[0]["StateReason"] = {"Code": "Server.InsufficientInstanceCapacity"}
        c.service.watchdog()
        c.coordinator()
        self.assertEqual(len(c.launches), 1)
        self.assertTrue(c.state["allocation_consumed"])
        self.assertEqual(c.state["phase"], "closing")

    def test_launch_response_preserves_root_identity_before_first_watchdog(self):
        c = Cloud()
        c.coordinator()
        c.instances[0]["State"]["Name"] = "terminated"
        c.instances[0]["BlockDeviceMappings"] = []
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        c.service.watchdog()
        self.assertEqual(c.state["cleanup"]["deleted_roots"], [ROOT])

    def missing_root(self):
        c = Cloud()
        def launch(**kw):
            response = c.run_instances(**kw)
            response["Instances"][0]["BlockDeviceMappings"] = []
            c.instances[0]["BlockDeviceMappings"] = []
            return response
        c.api["ec2"].run_instances.side_effect = launch
        c.coordinator()
        return c

    def test_tag_discovery_recovers_root_then_exact_id_absence_proves_deletion(self):
        c = self.missing_root()
        volume = dict(VolumeId=ROOT, Tags=c.instances[0]["Tags"],
                      Attachments=[dict(InstanceId=INSTANCE, Device="/dev/sda1")])
        c.api["ec2"].describe_volumes.side_effect = lambda **kw: {"Volumes": [volume]}
        c.service.watchdog()
        self.assertEqual(c.state["seen"][INSTANCE]["root_volumes"], [ROOT])
        filters = c.api["ec2"].describe_volumes.call_args.kwargs["Filters"]
        self.assertEqual({v["Name"] for v in filters}, {"tag:" + k for k in durable.TAG_KEYS})
        c.instances[0]["State"]["Name"] = "terminated"
        c.api["ec2"].describe_volumes.side_effect = c.describe_volumes
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        c.service.watchdog()
        self.assertEqual(c.state["cleanup"]["deleted_roots"], [ROOT])
        self.assertEqual(c.api["ec2"].describe_volumes.call_args.kwargs, {"VolumeIds": [ROOT]})

    def test_empty_tag_search_cannot_prove_an_unobserved_root_never_existed(self):
        c = self.missing_root()
        c.instances[0]["State"]["Name"] = "terminated"
        c.now = common.utc(c.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        with self.assertRaisesRegex(ValueError, "root inventory is incomplete"):
            c.service.watchdog()
        self.assertIsNone(c.state["cleanup"])
        self.assertTrue(c.api["ec2"].describe_volumes.called)

    def test_foreign_or_multiple_tagged_roots_cannot_be_claimed(self):
        for kind in ("foreign-tag", "foreign-attachment", "multiple"):
            with self.subTest(kind=kind):
                c = self.missing_root()
                volume = dict(VolumeId=ROOT, Tags=copy.deepcopy(c.instances[0]["Tags"]), Attachments=[])
                if kind == "foreign-tag":
                    volume["Tags"][0]["Value"] = "wrong"
                if kind == "foreign-attachment":
                    volume["Attachments"] = [{"InstanceId": "i-" + "9" * 17, "Device": "/dev/sda1"}]
                volumes = [volume]
                if kind == "multiple":
                    volumes.append(dict(volume, VolumeId="vol-" + "9" * 17))
                c.api["ec2"].describe_volumes.side_effect = None
                c.api["ec2"].describe_volumes.return_value = {"Volumes": volumes}
                with self.assertRaises(ValueError):
                    c.service.watchdog()
                self.assertFalse(c.state["worker_published"])
                self.assertIsNone(c.state["cleanup"])


if __name__ == "__main__":
    unittest.main()
