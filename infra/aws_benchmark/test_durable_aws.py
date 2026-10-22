"""Receipt-size and evidence-retention regressions using fake AWS clients only."""

from datetime import timedelta
import json
from pathlib import Path
import tempfile
import unittest

from infra.aws_benchmark import common, durable, durable_aws, durable_client
from infra.aws_benchmark.test_durable_coordinator import ApiError, Cloud, NOW
from infra.aws_benchmark.test_durable_client import Cloud as CollectorCloud, MACHINE


def maximum_rejections():
    cloud = Cloud()
    cloud.event["execution_arn"] = durable_client.execution_arn(MACHINE, cloud.request["run"])
    cloud.state["attempts"] = durable.MAX_ATTEMPTS - 1
    cloud.state["capacity_failures"] = [
        dict(attempt=i, at=common.timestamp(NOW + timedelta(seconds=(i - 1) * durable.RETRY_SECONDS)),
             request_id="a" * 64) for i in range(1, durable.MAX_ATTEMPTS)]
    cloud.now = NOW + timedelta(seconds=(durable.MAX_ATTEMPTS - 1) * durable.RETRY_SECONDS)
    cloud.guard["observed_at"] = common.timestamp(cloud.now)
    cloud.launch_error = ApiError("InsufficientInstanceCapacity", request_id="a" * 64)
    cloud.coordinator()
    cloud.now = common.utc(cloud.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
    cloud.service.watchdog()
    return cloud


class ReceiptTests(unittest.TestCase):
    def test_maximum_capacity_rejections_and_terminal_cleanup_are_collectible(self):
        cloud = maximum_rejections()
        state = cloud.state
        self.assertEqual(state["attempts"], durable.MAX_ATTEMPTS)
        self.assertEqual(len(state["capacity_failures"]), durable.MAX_ATTEMPTS)
        self.assertEqual(state["phase"], "complete")
        self.assertEqual(state["attempt"]["parameters"], cloud.launches[-1])
        expected = dict(protocol=durable.PROTOCOL, observed_at=state["cleanup"]["verified_at"], **state)
        self.assertGreater(len(json.dumps(expected).encode()), durable_client.MAX_RECEIPT)
        raw = cloud.objects[durable_aws.RECEIPT]
        self.assertLessEqual(len(raw), durable_client.MAX_RECEIPT)
        self.assertEqual(json.loads(raw), expected)
        self.assertEqual(durable_aws.MAX_RECEIPT, durable_client.MAX_RECEIPT)
        collector = CollectorCloud(cloud.request)
        collector.start(stateMachineArn=MACHINE, name=cloud.request["run"], input='{"run":"123-1"}')
        collector.execution["status"] = "FAILED"
        collector.objects.update({durable_aws.RECEIPT: raw, common.STATUS_KEY: cloud.objects[common.STATUS_KEY]})
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "collected"
            result = durable_client.collect(collector.api, cloud.config.bucket, MACHINE,
                                            cloud.request["run"], output, now=lambda: cloud.now)
            retained = (output / "durable.json").read_bytes()
        self.assertEqual(retained, raw)
        self.assertEqual(result["state"], "complete")
        self.assertEqual(result["cleanup"], "verified")
        self.assertEqual(result["conclusion"], "failure")
        self.assertFalse(result["benchmark_success"])
        self.assertTrue(all(len(proof["request_id"]) == 64 for proof in json.loads(retained)["capacity_failures"]))

    def test_full_receipt_limit_includes_wrapper_and_escaped_error_bytes(self):
        cloud = maximum_rejections()
        cloud.state["close_reason"] = 'diagnostic: é\n"'
        size = len(cloud.service.receipt_bytes(cloud.state))
        cloud.state["close_reason"] += "x" * (durable_client.MAX_RECEIPT - size)
        cloud.service.save(cloud.state, cloud.revision)
        cloud.service.receipt(cloud.state)
        retained = cloud.objects[durable_aws.RECEIPT]
        self.assertEqual(len(retained), durable_client.MAX_RECEIPT)
        self.assertEqual(json.loads(retained)["close_reason"], cloud.state["close_reason"])
        self.assertEqual(durable_client.read_json(cloud.api, cloud.config.bucket, durable_aws.RECEIPT),
                         json.loads(retained))
        cloud.state["close_reason"] += "x"
        self.assertLess(len(json.dumps(cloud.state, separators=(",", ":")).encode()), durable_client.MAX_RECEIPT)
        cloud.api["dynamodb"].update_item.reset_mock()
        cloud.api["s3"].put_object.reset_mock()
        with self.assertRaisesRegex(ValueError, "receipt exceeds collector bound"):
            cloud.service.save(cloud.state, cloud.revision)
        with self.assertRaisesRegex(ValueError, "receipt exceeds collector bound"):
            cloud.service.receipt(cloud.state)
        cloud.api["dynamodb"].update_item.assert_not_called()
        cloud.api["s3"].put_object.assert_not_called()
        self.assertEqual(cloud.objects[durable_aws.RECEIPT], retained)

    def test_in_progress_save_reserves_same_fixed_width_observation_wrapper(self):
        cloud = Cloud()
        cloud.state["close_reason"] = "guard failure"
        cloud.state["close_reason"] += "x" * (durable_client.MAX_RECEIPT - len(cloud.service.receipt_bytes(cloud.state)))
        cloud.service.save(cloud.state, cloud.revision)
        cloud.now += timedelta(days=1000, microseconds=999999)
        cloud.service.receipt(cloud.state)
        self.assertEqual(len(cloud.objects[durable_aws.RECEIPT]), durable_client.MAX_RECEIPT)
        self.assertEqual(json.loads(cloud.objects[durable_aws.RECEIPT])["observed_at"], common.timestamp(cloud.now))

    def test_nonfinite_state_cannot_be_saved_or_published(self):
        cloud = Cloud()
        cloud.state["close_reason"] = float("nan")
        with self.assertRaises(ValueError):
            cloud.service.save(cloud.state, cloud.revision)
        with self.assertRaises(ValueError):
            cloud.service.receipt(cloud.state)
        cloud.api["dynamodb"].update_item.assert_not_called()
        cloud.api["s3"].put_object.assert_not_called()

    def test_failed_terminal_write_is_repairable_without_changing_evidence(self):
        cloud = maximum_rejections()
        expected = cloud.objects[durable_aws.RECEIPT]
        cloud.api["s3"].put_object.side_effect = ApiError("ServiceUnavailable")
        with self.assertRaises(ApiError):
            cloud.service.receipt(cloud.state)
        self.assertEqual(cloud.objects[durable_aws.RECEIPT], expected)
        cloud.api["s3"].put_object.side_effect = cloud.put_object
        cloud.now += timedelta(days=1)
        cloud.service.watchdog()
        self.assertEqual(cloud.objects[durable_aws.RECEIPT], expected)


if __name__ == "__main__":
    unittest.main()
