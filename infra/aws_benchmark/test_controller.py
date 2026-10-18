"""Controller, admission and retry regressions without AWS access."""

from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from infra.aws_benchmark import common, controller, operator
from infra.aws_benchmark.test_safety import FakeCloud, Missing, NOW, closed, window


class ServiceError(Exception):
    def __init__(self, code, status=None):
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


class TransportError(ConnectionError):
    response = None


class RetryTests(unittest.TestCase):
    def test_transient_errors_retry_the_operation_and_keep_its_result(self):
        for error in (ServiceError("InternalError", 500), ServiceError("SlowDown", 503),
                      ServiceError("ThrottlingException", 429), TransportError("reset"), TimeoutError()):
            with self.subTest(error=type(error).__name__), patch.object(common.time, "sleep") as sleep:
                operation = Mock(side_effect=[error, b"complete"])
                budget = Mock()
                budget.left.return_value = 100
                self.assertEqual(common.retry_io(operation, budget, reserve=30), b"complete")
                self.assertEqual(operation.call_count, 2)
                sleep.assert_called_once_with(1)
                self.assertTrue(all(call.args == (30,) for call in budget.left.call_args_list))

    def test_retry_count_is_finite_when_errors_persist(self):
        operation = Mock(side_effect=TransportError("offline"))
        budget = Mock()
        budget.left.return_value = 100
        with patch.object(common.time, "sleep") as sleep, self.assertRaises(TransportError):
            common.retry_io(operation, budget)
        self.assertEqual(operation.call_count, 3)
        self.assertEqual([call.args for call in sleep.call_args_list], [(1,), (2,)])

    def test_access_missing_validation_and_body_format_errors_are_not_retried(self):
        for error in (ServiceError("AccessDenied", 403), Missing(), ValueError("bad digest"),
                      json.JSONDecodeError("bad json", "!", 0), RuntimeError("other")):
            with self.subTest(error=type(error).__name__), patch.object(common.time, "sleep") as sleep:
                operation = Mock(side_effect=error)
                with self.assertRaises(type(error)):
                    common.retry_io(operation, Mock())
                operation.assert_called_once()
                sleep.assert_not_called()

    def test_transport_response_none_does_not_hide_original_error(self):
        error = TransportError("connection lost")
        self.assertIsNone(common.error_code(error))
        self.assertTrue(common.retryable_io(error))

    def test_budget_exhaustion_prevents_next_attempt_and_does_not_sleep_past_reserve(self):
        operation = Mock(side_effect=TransportError("offline"))
        budget = Mock()
        budget.left.side_effect = [2, 1]
        with patch.object(common.time, "sleep") as sleep, self.assertRaisesRegex(TimeoutError, "retry"):
            common.retry_io(operation, budget, reserve=30)
        operation.assert_called_once()
        sleep.assert_not_called()

    def test_expired_budget_prevents_initial_operation(self):
        operation = Mock()
        budget = Mock()
        budget.left.side_effect = TimeoutError("deadline")
        with self.assertRaisesRegex(TimeoutError, "deadline"):
            common.retry_io(operation, budget)
        operation.assert_not_called()

    def test_success_after_budget_is_exhausted_does_not_restart_operation(self):
        operation = Mock(return_value=b"finished late")
        budget = Mock()
        budget.left.side_effect = [5, TimeoutError("deadline")]
        with self.assertRaisesRegex(TimeoutError, "deadline"):
            common.retry_io(operation, budget)
        operation.assert_called_once()

    def test_body_read_failure_closes_body_and_retries_entire_get(self):
        broken = Mock()
        broken.read.side_effect = TransportError("body reset")
        recovered = io.BytesIO(b"archive")
        s3 = Mock()
        s3.get_object.side_effect = [
            {"ContentLength": 7, "Body": broken}, {"ContentLength": 7, "Body": recovered}]
        budget = Mock()
        budget.left.return_value = 100
        with patch.object(common.time, "sleep"):
            data = common.retry_io(lambda: common.get_bytes(s3, "bucket", common.RESULTS_KEY, 10), budget)
        self.assertEqual(data, b"archive")
        self.assertEqual(s3.get_object.call_count, 2)
        broken.close.assert_called_once()
        self.assertTrue(recovered.closed)


class AdmissionTests(unittest.TestCase):
    def test_operator_rejects_short_windows_before_any_cloud_operation(self):
        for seconds in (300, 305, 600, common.MIN_WINDOW - 1):
            lease = window()
            lease["deadline"] = common.timestamp(common.utc(lease["opens"]) + timedelta(seconds=seconds))
            cloud = FakeCloud()
            with self.subTest(seconds=seconds), self.assertRaisesRegex(ValueError, "window must be"):
                operator.arm(cloud.api, "group", "bucket", lease, now=lambda: NOW)
            for api in cloud.api.values():
                self.assertEqual(api.mock_calls, [])

    def test_minimum_window_arms_with_each_allowed_opening_lead(self):
        for lead in (120, 180, common.MAX_OPEN_LEAD):
            lease = window()
            lease.update(opens=common.timestamp(NOW + timedelta(seconds=lead)),
                         deadline=common.timestamp(NOW + timedelta(seconds=lead + common.MIN_WINDOW)))
            cloud = FakeCloud()
            operator.arm(cloud.api, "group", "bucket", lease, now=lambda: NOW)
            self.assertEqual(cloud.events[-1], "open-123-1")
            self.assertEqual(json.loads(cloud.objects[common.STATUS_KEY]), {"run": "123-1", "state": "pending"})

    def test_pending_status_must_read_back_before_opening(self):
        cloud = FakeCloud()
        def get(**kwargs):
            if kwargs["Key"] == common.STATUS_KEY:
                return {"ContentLength": 2, "Body": io.BytesIO(b"{}")}
            return cloud.get(**kwargs)
        cloud.api["s3"].get_object.side_effect = get
        with self.assertRaisesRegex(ValueError, "pending status read-back"):
            operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
        self.assertEqual([a["MaxSize"] for a in cloud.actions], [0])
        self.assertNotIn("open-123-1", cloud.events)


def controller_cloud(lease=None):
    cloud = FakeCloud()
    lease = lease or window(datetime.now(timezone.utc) - timedelta(minutes=4))
    cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
    cloud.objects[common.STATUS_KEY] = json.dumps({"run": lease["run"], "state": "pending"}).encode()
    cloud.actions = [dict(ScheduledActionName="close-" + lease["run"], StartTime=lease["deadline"],
                         MinSize=0, DesiredCapacity=0, MaxSize=0)]
    cloud.api["autoscaling"].describe_auto_scaling_groups.return_value = {"AutoScalingGroups": [closed(1)]}
    return cloud, {k: lease[k] for k in ("run", "baseline", "candidate", "suite")}


def run_controller(cloud, identity, root):
    for key in common.BINARIES:
        (root / key).parent.mkdir(exist_ok=True)
        (root / key).write_bytes(b"binary")
    controller.run(cloud.api, "group", "bucket", identity, root, root / "out")


class ControllerMailboxTests(unittest.TestCase):
    def test_minimum_window_can_launch_after_two_minute_scheduled_delay_and_poll_margin(self):
        for poll_margin in (0, 5, 10):
            now = datetime.now(timezone.utc)
            lease = window(now)
            opens = now - timedelta(seconds=120 + poll_margin)
            lease.update(opens=common.timestamp(opens),
                         deadline=common.timestamp(opens + timedelta(seconds=common.MIN_WINDOW)))
            cloud, identity = controller_cloud(lease)
            def launched(**kwargs):
                if kwargs["DesiredCapacity"] == 1:
                    cloud.objects[common.STATUS_KEY] = json.dumps(
                        {"run": identity["run"], "state": "completed", "success": True}).encode()
            cloud.api["autoscaling"].set_desired_capacity.side_effect = launched
            with self.subTest(poll_margin=poll_margin), tempfile.TemporaryDirectory() as directory:
                run_controller(cloud, identity, Path(directory))
            self.assertEqual([c.kwargs["DesiredCapacity"] for c in
                              cloud.api["autoscaling"].set_desired_capacity.call_args_list], [1, 0])

    def test_transient_status_and_archive_reads_preserve_completed_results(self):
        for status_error in (ServiceError("InternalError", 500), ServiceError("SlowDown", 503),
                             TransportError("reset"), TimeoutError("stall")):
            cloud, identity = controller_cloud()
            archive = b"completed results"
            terminal = {"run": identity["run"], "state": "completed", "success": True,
                        "sha256": common.digest(archive)}
            status_reads = [None, status_error, terminal]
            result_reads = [TransportError("archive reset"), archive]
            def get(**kwargs):
                if kwargs["Key"] == common.STATUS_KEY:
                    value = status_reads.pop(0)
                    if isinstance(value, Exception):
                        raise value
                    if value is not None:
                        data = json.dumps(value).encode()
                        return {"ContentLength": len(data), "Body": io.BytesIO(data)}
                if kwargs["Key"] == common.RESULTS_KEY:
                    value = result_reads.pop(0)
                    if isinstance(value, Exception):
                        raise value
                    return {"ContentLength": len(value), "Body": io.BytesIO(value)}
                return cloud.get(**kwargs)
            cloud.api["s3"].get_object.side_effect = get
            with self.subTest(error=type(status_error).__name__), tempfile.TemporaryDirectory() as directory, \
                 patch.object(common.time, "sleep"):
                root = Path(directory)
                run_controller(cloud, identity, root)
                self.assertEqual((root / "out/results.zip").read_bytes(), archive)
                self.assertEqual(json.loads((root / "out/status.json").read_text()), terminal)
            self.assertEqual([c.kwargs["DesiredCapacity"] for c in
                              cloud.api["autoscaling"].set_desired_capacity.call_args_list], [1, 0])
            self.assertEqual(status_reads, [])
            self.assertEqual(result_reads, [])

    def test_denied_or_missing_pending_status_never_requests_capacity(self):
        for error in (ServiceError("AccessDenied", 403), Missing()):
            cloud, identity = controller_cloud()
            def get(**kwargs):
                if kwargs["Key"] == common.STATUS_KEY:
                    raise error
                return cloud.get(**kwargs)
            cloud.api["s3"].get_object.side_effect = get
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as directory, \
                 self.assertRaises(type(error)):
                run_controller(cloud, identity, Path(directory))
            cloud.api["autoscaling"].set_desired_capacity.assert_not_called()

    def test_denied_status_after_launch_fails_immediately_and_tears_down(self):
        cloud, identity = controller_cloud()
        count = 0
        def get(**kwargs):
            nonlocal count
            if kwargs["Key"] == common.STATUS_KEY:
                count += 1
                if count > 1:
                    raise ServiceError("AccessDenied", 403)
            return cloud.get(**kwargs)
        cloud.api["s3"].get_object.side_effect = get
        with tempfile.TemporaryDirectory() as directory, patch.object(common.time, "sleep") as sleep, \
             self.assertRaises(ServiceError):
            run_controller(cloud, identity, Path(directory))
        self.assertEqual(count, 2)
        sleep.assert_not_called()
        self.assertEqual([c.kwargs["DesiredCapacity"] for c in
                          cloud.api["autoscaling"].set_desired_capacity.call_args_list], [1, 0])

    def test_pending_status_polls_without_publishing_failure(self):
        cloud, identity = controller_cloud()
        count = 0
        def get(**kwargs):
            nonlocal count
            if kwargs["Key"] == common.STATUS_KEY:
                count += 1
                if count == 3:
                    cloud.objects[common.STATUS_KEY] = json.dumps(
                        {"run": identity["run"], "state": "completed", "success": True}).encode()
            return cloud.get(**kwargs)
        cloud.api["s3"].get_object.side_effect = get
        with tempfile.TemporaryDirectory() as directory, patch.object(common.time, "sleep") as sleep:
            run_controller(cloud, identity, Path(directory))
            self.assertTrue(json.loads((Path(directory) / "out/status.json").read_text())["success"])
        sleep.assert_called_once_with(10)


if __name__ == "__main__":
    unittest.main()
