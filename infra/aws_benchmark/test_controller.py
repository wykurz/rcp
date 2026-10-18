"""Controller, admission and retry regressions without AWS access."""

from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import types
from urllib.error import HTTPError, URLError
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

    def test_success_is_preserved_after_admission_reserve_has_been_consumed(self):
        for reserve in (0, 5, 30, common.RESERVE):
            operation = Mock(return_value=b"finished")
            budget = Mock()
            budget.left.side_effect = [5, TimeoutError("reserve consumed")]
            self.assertEqual(common.retry_io(operation, budget, reserve), b"finished")
            operation.assert_called_once()
            budget.left.assert_called_once_with(reserve)

    def test_short_successful_body_is_retried_and_both_streams_are_closed(self):
        partial, complete = io.BytesIO(b"part"), io.BytesIO(b"complete")
        s3 = Mock()
        s3.get_object.side_effect = [{"ContentLength": 8, "Body": partial},
                                     {"ContentLength": 8, "Body": complete}]
        budget = Mock()
        budget.left.return_value = 100
        with patch.object(common.time, "sleep"):
            result = common.retry_io(lambda: common.get_bytes(s3, "bucket", "key", 8), budget)
        self.assertEqual(result, b"complete")
        self.assertEqual(s3.get_object.call_count, 2)
        self.assertTrue(partial.closed and complete.closed)

    def test_oidc_transport_and_server_errors_retry_but_denial_does_not(self):
        for error, expected in ((URLError("temporary DNS failure"), True),
                                (HTTPError("offline", 503, "busy", {}, None), True),
                                (HTTPError("offline", 403, "denied", {}, None), False)):
            self.assertEqual(common.retryable_io(error), expected)
            if isinstance(error, HTTPError):
                error.close()

    def test_real_sdk_and_streaming_tls_errors_are_retryable(self):
        try:
            from botocore.exceptions import SSLError, ProxyConnectionError
            from urllib3.exceptions import SSLError as BodySSLError
        except ImportError:
            self.skipTest("run this transport contract with the pinned AWS requirements installed")
        errors = (SSLError(endpoint_url="offline", error="injected TLS handshake"),
                  ProxyConnectionError(proxy_url="offline", error="injected proxy failure"),
                  BodySSLError("injected TLS body record"))
        for error in errors:
            operation = Mock(side_effect=[error, b"complete"])
            budget = Mock()
            budget.left.return_value = 100
            with patch.object(common.time, "sleep"):
                self.assertEqual(common.retry_io(operation, budget), b"complete")
            self.assertEqual(operation.call_count, 2)

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
    cloud.objects[common.STATUS_KEY] = json.dumps(common.pending_status(lease["run"])).encode()
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


class RevisedContractsTests(unittest.TestCase):
    def test_request_schema_rejects_unknown_fields_and_non_digest_values(self):
        lease = window()
        request = {key: lease[key] for key in ("run", "baseline", "candidate", "suite", "deadline")}
        request["sha256"] = {key: "a" * 64 for key in common.BINARIES}
        self.assertIs(common.validate_request(request, lease), request)
        for mutation in ({"extra": "ignored"}, {"sha256": {}},
                         {"sha256": {key: 7 for key in common.BINARIES}},
                         {"sha256": {key: "A" * 64 for key in common.BINARIES}},
                         {"sha256": {key: "a" * 63 for key in common.BINARIES}},
                         {"run": "456-1"}):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                common.validate_request({**request, **mutation}, lease)

    def test_window_rejects_fields_that_would_expand_untrusted_metadata(self):
        with self.assertRaisesRegex(ValueError, "unexpected window fields"):
            common.validate_window({**window(), "extra": [[0]]}, NOW)

    def test_controller_refuses_late_admission_without_requesting_capacity(self):
        for remaining in (301, 320, 420, common.LAUNCH_RESERVE - 1):
            now = datetime.now(timezone.utc)
            lease = window(now)
            deadline = now + timedelta(seconds=remaining)
            lease.update(opens=common.timestamp(deadline - timedelta(seconds=common.MIN_WINDOW)),
                         deadline=common.timestamp(deadline))
            cloud, identity = controller_cloud(lease)
            with tempfile.TemporaryDirectory() as directory, self.assertRaises(ValueError):
                run_controller(cloud, identity, Path(directory))
            cloud.api["autoscaling"].set_desired_capacity.assert_not_called()

    def test_final_admission_check_runs_after_slow_prelaunch_describes(self):
        cloud, identity = controller_cloud()
        budget = Mock()
        budget.left.return_value = 1000
        count = 0
        def describe(**kwargs):
            nonlocal count
            count += 1
            if count == 2:
                budget.left.side_effect = TimeoutError("launch floor consumed by describe")
            return {"AutoScalingGroups": [closed(1)]}
        cloud.api["autoscaling"].describe_auto_scaling_groups.side_effect = describe
        with tempfile.TemporaryDirectory() as directory, patch.object(controller, "Budget", return_value=budget), \
             self.assertRaisesRegex(TimeoutError, "launch floor"):
            run_controller(cloud, identity, Path(directory))
        cloud.api["autoscaling"].set_desired_capacity.assert_not_called()

    def test_unknown_matching_terminal_status_is_saved_compactly_before_rejection(self):
        for state in (None, "old-completed"):
            cloud, identity = controller_cloud()
            status = {"run": identity["run"], "success": False, "error": "old worker"}
            if state is not None:
                status["state"] = state
            def launch(**kwargs):
                if kwargs["DesiredCapacity"] == 1:
                    cloud.objects[common.STATUS_KEY] = json.dumps(status).encode()
            cloud.api["autoscaling"].set_desired_capacity.side_effect = launch
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with self.assertRaisesRegex(ValueError, "protocol mismatch; rebuild WorkerAmiId"):
                    run_controller(cloud, identity, root)
                text = (root / "out/status.json").read_text()
                self.assertEqual(json.loads(text), status)
                self.assertNotIn("\n", text)
            self.assertEqual([c.kwargs["DesiredCapacity"] for c in
                              cloud.api["autoscaling"].set_desired_capacity.call_args_list], [1, 0])

    def test_completed_results_crossing_download_reserve_are_preserved(self):
        cloud, identity = controller_cloud()
        archive = b"completed archive"
        def launch(**kwargs):
            if kwargs["DesiredCapacity"] == 1:
                cloud.objects[common.STATUS_KEY] = json.dumps({
                    **common.completed_status(identity["run"], True), "sha256": common.digest(archive)}).encode()
        cloud.api["autoscaling"].set_desired_capacity.side_effect = launch
        budget = Mock()
        budget.left.return_value = 1000
        def get(**kwargs):
            if kwargs["Key"] == common.RESULTS_KEY:
                budget.left.side_effect = TimeoutError("download reserve consumed")
                return {"ContentLength": len(archive), "Body": io.BytesIO(archive)}
            return cloud.get(**kwargs)
        cloud.api["s3"].get_object.side_effect = get
        with tempfile.TemporaryDirectory() as directory, patch.object(controller, "Budget", return_value=budget):
            root = Path(directory)
            run_controller(cloud, identity, root)
            self.assertEqual((root / "out/results.zip").read_bytes(), archive)

    def test_each_safe_controller_operation_retries_one_transient_failure(self):
        cases = [
            ("window", "s3", "get_object", 1, common.WINDOW_KEY),
            ("pending", "s3", "get_object", 1, common.STATUS_KEY),
            ("binary", "s3", "put_object", 1, "input/baseline/rcp"),
            ("request", "s3", "put_object", 1, common.REQUEST_KEY),
            ("wait schedule", "autoscaling", "describe_scheduled_actions", 1, None),
            ("final schedule", "autoscaling", "describe_scheduled_actions", 2, None),
            ("wait group", "autoscaling", "describe_auto_scaling_groups", 1, None),
            ("final group", "autoscaling", "describe_auto_scaling_groups", 2, None),
            ("instances", "ec2", "describe_instances", 1, None),
        ]
        for label, client, method_name, occurrence, key in cases:
            cloud, identity = controller_cloud()
            method = getattr(cloud.api[client], method_name)
            original, returned = method.side_effect, method.return_value
            matched = 0
            injected = False
            def operation(**kwargs):
                nonlocal matched, injected
                if key is None or kwargs.get("Key") == key:
                    matched += 1
                    if matched == occurrence:
                        injected = True
                        raise ServiceError("SlowDown", 503)
                return original(**kwargs) if callable(original) else returned
            method.side_effect = operation
            def launch(**kwargs):
                if kwargs["DesiredCapacity"] == 1:
                    cloud.objects[common.STATUS_KEY] = json.dumps(common.completed_status(identity["run"], True)).encode()
            cloud.api["autoscaling"].set_desired_capacity.side_effect = launch
            with self.subTest(operation=label), tempfile.TemporaryDirectory() as directory, \
                 patch.object(common.time, "sleep") as sleep:
                run_controller(cloud, identity, Path(directory))
                self.assertTrue(injected)
                sleep.assert_called_once_with(1)
            self.assertEqual([c.kwargs["DesiredCapacity"] for c in
                              cloud.api["autoscaling"].set_desired_capacity.call_args_list], [1, 0])

    def test_oidc_and_sts_bootstrap_retries_are_bounded_without_cloud_calls(self):
        session, sts = Mock(), Mock()
        boto3 = types.SimpleNamespace(client=Mock(return_value=sts), Session=Mock(return_value=session))
        config = types.SimpleNamespace(Config=Mock(return_value="single-attempt-config"))
        credentials = {"AccessKeyId": "dummy", "SecretAccessKey": "dummy", "SessionToken": "dummy"}
        sts.assume_role_with_web_identity.side_effect = [ServiceError("Throttling", 429), {"Credentials": credentials}]
        response = io.BytesIO(b'{"value":"dummy-token"}')
        environment = {"ACTIONS_ID_TOKEN_REQUEST_URL": "https://example.invalid?request=1",
                       "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "dummy", "GITHUB_RUN_ID": "123"}
        with patch.dict("sys.modules", {"boto3": boto3, "botocore.config": config}), \
             patch.dict(controller.os.environ, environment), \
             patch.object(controller.urllib.request, "urlopen", side_effect=[URLError("temporary"), response]) as token, \
             patch.object(controller, "checked_clients", return_value={"safe": True}) as clients, \
             patch.object(common.time, "sleep") as sleep:
            self.assertEqual(controller.oidc_clients("us-east-1", "role"), {"safe": True})
        self.assertEqual(token.call_count, 2)
        self.assertEqual(sts.assume_role_with_web_identity.call_count, 2)
        self.assertEqual([call.args for call in sleep.call_args_list], [(1,), (1,)])
        clients.assert_called_once_with(session, "single-attempt-config")

    def test_operator_retries_fixed_key_writes_without_repeating_schedule_mutations(self):
        for key in (common.WINDOW_KEY, common.STATUS_KEY):
            cloud = FakeCloud()
            injected = False
            def put(**kwargs):
                nonlocal injected
                if kwargs["Key"] == key and not injected:
                    injected = True
                    raise ServiceError("SlowDown", 503)
                return cloud.put(**kwargs)
            cloud.api["s3"].put_object.side_effect = put
            with patch.object(common.time, "sleep") as sleep:
                operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
            self.assertTrue(injected)
            self.assertEqual([a["ScheduledActionName"] for a in cloud.actions], ["close-123-1", "open-123-1"])
            sleep.assert_called_once_with(1)

    def test_operator_does_not_retry_uncertain_schedule_mutations(self):
        for call in (1, 2):
            cloud = FakeCloud()
            count = 0
            def schedule(**kwargs):
                nonlocal count
                count += 1
                cloud.schedule(**kwargs)
                if count == call:
                    raise TransportError("uncertain schedule result")
            cloud.api["autoscaling"].put_scheduled_update_group_action.side_effect = schedule
            with self.assertRaises(TransportError), patch.object(common.time, "sleep") as sleep:
                operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
            self.assertEqual(count, call)
            sleep.assert_not_called()
            cloud.api["autoscaling"].set_desired_capacity.assert_not_called()


class OperatorPrerequisiteTests(unittest.TestCase):
    def test_disabled_management_setting_uses_exact_group_account_and_region(self):
        cloud = FakeCloud()
        operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
        arn = cloud.api["autoscaling"].describe_auto_scaling_groups.return_value["AutoScalingGroups"][0]["AutoScalingGroupARN"]
        parts = arn.split(":", 5)
        expected = f"arn:{parts[1]}:ssm:{parts[3]}:{parts[4]}:servicesetting/ssm/managed-instance/default-ec2-instance-management-role"
        cloud.api["ssm"].get_service_setting.assert_called_once_with(SettingId=expected)

    def test_enabled_unknown_or_missing_management_setting_blocks_before_mutations(self):
        changes = ({"SettingValue": ""}, {"SettingValue": None},
                   {"SettingValue": "arn:aws:iam::123456789012:role/managed"},
                   {"Status": "Customized"}, {"Status": "PendingUpdate"},
                   {"SettingId": "/unexpected"})
        for change in changes:
            cloud = FakeCloud()
            setting = cloud.api["ssm"].get_service_setting.return_value["ServiceSetting"]
            setting.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
            cloud.api["autoscaling"].suspend_processes.assert_not_called()
            self.assertEqual(cloud.events, [])
        for missing in ("SettingId", "SettingValue", "Status"):
            cloud = FakeCloud()
            del cloud.api["ssm"].get_service_setting.return_value["ServiceSetting"][missing]
            with self.subTest(missing=missing), self.assertRaises(ValueError):
                operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
            self.assertEqual(cloud.events, [])

    def test_management_read_retries_transient_but_not_authorization_failure(self):
        cloud = FakeCloud()
        expected = cloud.api["ssm"].get_service_setting.return_value
        cloud.api["ssm"].get_service_setting.side_effect = [ServiceError("ThrottlingException", 429), expected]
        with patch.object(common.time, "sleep") as sleep:
            operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
        sleep.assert_called_once_with(1)
        cloud = FakeCloud()
        cloud.api["ssm"].get_service_setting.side_effect = ServiceError("AccessDeniedException", 403)
        with self.assertRaises(ServiceError):
            operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
        cloud.api["ssm"].get_service_setting.assert_called_once()
        self.assertEqual(cloud.events, [])


if __name__ == "__main__":
    unittest.main()
