"""Real response shapes and role-aware offline coverage for allocation recovery."""

import base64
import copy
from datetime import timedelta
import json
import unittest
from unittest.mock import patch
from xml.sax.saxutils import escape

import botocore.parsers
import botocore.session

from . import common, durable
from .test_durable_coordinator import ApiError, INSTANCE, ROOT
from .test_production_errors import cancellation
from .test_production_runtime import ProductionCloud
from .test_production_stack import BUCKET_ARN, permits


class LaunchOutcomeTests(unittest.TestCase):
    def setUp(self):
        network = patch("socket.create_connection", side_effect=AssertionError("offline tests only"))
        network.start()
        self.addCleanup(network.stop)

    def test_floor_runtime_retries_same_token_after_transient_send(self):
        request = durable.request
        def floor(*args, **kwargs):
            return request(*args, **kwargs, runtime_seconds=durable.minimum_runtime())
        with patch.object(durable, "request", side_effect=floor):
            cloud = ProductionCloud()
        sent = []
        def run(**parameters):
            sent.append(copy.deepcopy(parameters))
            if len(sent) == 1:
                cloud.now += timedelta(seconds=1)
                raise TimeoutError("response lost")
            return cloud.run_instances(**parameters)
        cloud.api["ec2"].run_instances.side_effect = run
        cloud.coordinator()
        self.assertEqual(len(sent), 2)
        self.assertEqual(sent[0], sent[1])
        self.assertEqual(cloud.state["launch_outcome"], "accepted")

    def test_late_same_token_recovery_does_not_grant_useful_runtime(self):
        cloud = ProductionCloud()
        cloud.launch_error = TimeoutError("unknown earlier send")
        cloud.coordinator()
        original = copy.deepcopy(cloud.state["attempt"])
        cloud.now += timedelta(seconds=700)
        cloud.guard["observed_at"] = common.timestamp(cloud.now)
        cloud.launch_error = None
        cloud.coordinator()
        self.assertEqual(cloud.state["attempt"], original)
        self.assertEqual(cloud.state["attempts"], 1)
        self.assertEqual(cloud.state["launch_sender"]["sequence"], 2)
        with self.assertRaisesRegex(ValueError, "remaining allocation time"):
            cloud.service.watchdog()
        self.assertFalse(cloud.state["worker_published"])
        self.assertIn(("terminate", [INSTANCE]), cloud.events)
        cloud.complete()
        self.assertEqual(cloud.state["phase"], "complete")

    def test_first_definitive_client_rejection_closes_and_retires_no_allocation(self):
        for code in ("VcpuLimitExceeded", "UnauthorizedOperation", "InvalidParameterValue"):
            with self.subTest(code=code):
                cloud = ProductionCloud()
                cloud.launch_error = ApiError(code, status=400)
                cloud.coordinator()
                self.assertEqual(len(cloud.launches), 1)
                self.assertEqual(cloud.state["launch_outcome"], "rejected")
                self.assertFalse(cloud.state["allocation_consumed"])
                self.assertEqual(cloud.state["phase"], "closing")
                self.assertEqual(cloud.state["launch_rejection"]["code"], code)
                cloud.now = common.utc(cloud.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
                cloud.service.watchdog()
                self.assertEqual(cloud.state["phase"], "complete")
                self.assertEqual(cloud.state["cleanup"]["deleted_roots"], [])

    def test_ambiguous_prior_send_and_idempotency_errors_never_become_rejections(self):
        for error in (ApiError("IdempotentParameterMismatch", status=400),
                      ApiError("IdempotentInstanceTerminated", status=400),
                      ApiError("UnauthorizedOperation", request_id="", status=400)):
            with self.subTest(error=error):
                cloud = ProductionCloud()
                cloud.launch_error = error
                cloud.coordinator()
                self.assertEqual(cloud.state["launch_outcome"], "uncertain")
        cloud = ProductionCloud()
        cloud.launch_error = TimeoutError("possibly allocated")
        cloud.coordinator()
        original = copy.deepcopy(cloud.state["attempt"])
        for error in (ApiError("UnauthorizedOperation", status=400), ApiError("InsufficientInstanceCapacity", status=500)):
            cloud.now += timedelta(seconds=700)
            cloud.guard["observed_at"] = common.timestamp(cloud.now)
            cloud.launch_error = error
            cloud.coordinator()
            self.assertEqual(cloud.state["launch_outcome"], "uncertain")
            self.assertEqual(cloud.state["attempt"], original)
            self.assertEqual(cloud.state["capacity_failures"], [])

    def test_unsent_intent_revision_conflict_reconciles_after_concurrent_closure(self):
        cloud = ProductionCloud()
        original = cloud.database.transact_write_items
        first = True
        def write(**parameters):
            nonlocal first
            reply = original(**parameters)
            if first:
                first = False
                item = cloud.database.items[cloud.owner.pk]
                state = json.loads(item["state"]["S"])
                state.update(phase="closing", close_reason="production kill switch is off")
                item["state"] = {"S": json.dumps(state)}
                item["revision"] = {"N": str(int(item["revision"]["N"]) + 1)}
                raise TimeoutError("intent response lost after a concurrent close")
            return reply
        cloud.api["dynamodb"].transact_write_items.side_effect = write
        with self.assertRaises(common.RevisionConflict):
            cloud.coordinator()
        self.assertEqual(cloud.launches, [])
        self.assertEqual(cloud.state["launch_outcome"], "not-sent")
        self.assertFalse(cloud.state["allocation_consumed"])
        cloud.now = common.utc(cloud.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "complete")

    def test_revision_rejection_and_unavailable_readback_do_not_kill_healthy_worker(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        def conflict():
            cloud.database.before_write = None
            item = cloud.database.items[cloud.owner.pk]
            item["revision"] = {"N": str(int(item["revision"]["N"]) + 1)}
            cloud.database.read_error = TimeoutError("readback unavailable")
            raise cancellation("None", "ConditionalCheckFailed")
        cloud.database.before_write = conflict
        self.assertEqual(cloud.service.watchdog()["state"], "deferred")
        self.assertFalse(any(event[0] == "terminate" for event in cloud.events))
        cloud.database.read_error = None
        self.assertIsNone(cloud.state["close_reason"])
        cloud.service.watchdog()
        self.assertTrue(cloud.state["worker_published"])

    def test_botocore_parsed_terminal_empty_network_sets_reconcile_but_do_not_bind(self):
        cloud = ProductionCloud()
        cloud.launch_error, cloud.allocate_on_error = TimeoutError("lost response"), True
        cloud.coordinator()
        instance = cloud.instances[0]
        tags = "".join("<item><key>" + escape(tag["Key"]) + "</key><value>" + escape(tag["Value"])
                       + "</value></item>" for tag in instance["Tags"])
        xml = ("<DescribeInstancesResponse xmlns='http://ec2.amazonaws.com/doc/2016-11-15/'>"
               "<requestId>request-123</requestId><reservationSet><item><instancesSet><item>"
               "<instanceId>" + INSTANCE + "</instanceId><imageId>" + instance["ImageId"] + "</imageId>"
               "<instanceType>c5d.24xlarge</instanceType><clientToken>" + instance["ClientToken"] + "</clientToken>"
               "<launchTime>" + common.timestamp(instance["LaunchTime"]) + "</launchTime>"
               "<instanceState><code>48</code><name>terminated</name></instanceState>"
               "<groupSet/><networkInterfaceSet/><blockDeviceMapping/><tagSet>" + tags + "</tagSet>"
               "</item></instancesSet></item></reservationSet></DescribeInstancesResponse>").encode()
        shape = botocore.session.get_session().get_service_model("ec2").operation_model("DescribeInstances").output_shape
        parsed = botocore.parsers.create_parser("ec2").parse(dict(status_code=200, headers={}, body=xml), shape)
        observed = parsed["Reservations"][0]["Instances"][0]
        self.assertEqual(observed["SecurityGroups"], [])
        self.assertEqual(observed["NetworkInterfaces"], [])
        durable.validate_allocation_instance(cloud.request, observed, cloud.state["attempt"])
        with self.assertRaisesRegex(ValueError, "not eligible"):
            durable.bind(cloud.request, observed, cloud.state["attempt"])
        observed["SecurityGroups"] = [{"GroupId": "wrong"}]
        with self.assertRaisesRegex(ValueError, "network or profile"):
            durable.validate_allocation_instance(cloud.request, observed, cloud.state["attempt"])

    def test_watchdog_output_capture_obeys_actual_role_without_archive_or_version_reads(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        paths = cloud.paths()
        raw = json.dumps(dict(common.completed_status("123-1", True), sha256="a" * 64)).encode()
        cloud.objects[paths.status], cloud.object_versions[paths.status] = raw, "status-v1"
        calls = []
        def checked(method, original):
            def call(**parameters):
                key = parameters.get("Key", parameters.get("Prefix"))
                action = "s3:ListBucket" if method == "list" else (
                    "s3:GetObjectVersion" if "VersionId" in parameters else "s3:GetObject")
                resource = BUCKET_ARN if method == "list" else BUCKET_ARN + "/" + key
                context = {"s3:prefix": key, "s3:max-keys": parameters.get("MaxKeys", 1)} if method == "list" else {}
                self.assertTrue(permits("WatchdogRole", action, resource, context), (method, action, key))
                calls.append((method, key))
                return original(**parameters)
            return call
        cloud.api["s3"].get_object.side_effect = checked("get", cloud.get_object)
        cloud.api["s3"].head_object.side_effect = checked("head", cloud.head_object)
        cloud.api["s3"].list_objects_v2.side_effect = checked("list", cloud.list_objects_v2)
        state = cloud.state
        self.assertTrue(cloud.service.read_worker_status(state)["success"])
        self.assertEqual(base64.b64decode(state["worker_output"]["data_base64"]), raw)
        first = list(calls)
        cloud.objects.clear()
        self.assertTrue(cloud.service.read_worker_status(state)["success"])
        self.assertEqual(calls, first)
        self.assertFalse(any(method == "head" or key == paths.results for method, key in calls))


if __name__ == "__main__":
    unittest.main()
