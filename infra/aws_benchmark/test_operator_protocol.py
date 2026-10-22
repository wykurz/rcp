"""Offline legacy admission rejects direct or historical control and startup."""

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from infra.aws_benchmark import controller, operator
from infra.aws_benchmark.test_durable_worker import direct_window, durable_window
from infra.aws_benchmark.test_results import binding
from infra.aws_benchmark.test_safety import FakeCloud, NOW, startup, window


class OperatorProtocolTests(unittest.TestCase):
    def test_protocol_bearing_windows_fail_before_any_operator_read_or_write(self):
        for value in (direct_window(), durable_window(), {**window(), "protocol": None}):
            cloud = FakeCloud()
            with self.subTest(protocol=value["protocol"]), self.assertRaisesRegex(ValueError, "protocol"):
                operator.arm(cloud.api, "group", "bucket", value, now=lambda: NOW)
            self.assertFalse(any(api.mock_calls for api in cloud.api.values()))

    def test_controller_rejects_protocol_before_upload_or_capacity(self):
        for value in (direct_window(), durable_window(), {**window(), "protocol": None}):
            cloud = FakeCloud()
            cloud.objects["control/window.json"] = json.dumps(value).encode()
            with self.subTest(protocol=value["protocol"]), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with self.assertRaisesRegex(ValueError, "protocol"):
                    controller.run(cloud.api, "group", "bucket", binding(value["suite"]), root, root / "out")
                cloud.api["s3"].put_object.assert_not_called()
                cloud.api["autoscaling"].set_desired_capacity.assert_not_called()

    def test_legacy_cli_prints_exact_owner_receipt_without_changing_window_schema(self):
        value = window()
        args = ["operator", "--region", "us-east-1", "--asg", "group", "--bucket", "bucket"]
        for field, item in value.items():
            args.extend(["--" + field, item])
        owner, api, output = {"token": "unique", "request_sha256": "f" * 64}, Mock(), io.StringIO()
        with patch("sys.argv", args), patch.object(operator, "operator_clients", return_value=api), \
             patch.object(operator, "arm", return_value=owner) as arm, redirect_stdout(output):
            operator.main()
        receipt, _ = json.JSONDecoder().raw_decode(output.getvalue())
        self.assertEqual(receipt, {"window": value, "mailbox_owner": owner})
        arm.assert_called_once_with(api, "group", "bucket", value)

    def test_management_arn_must_bind_the_exact_returned_account_and_region(self):
        for arn in (None, "arn:aws:ssm:us-west-2:123456789012:servicesetting/"
                          "ssm/managed-instance/default-ec2-instance-management-role",
                    "arn:aws:ssm:us-east-1:999999999999:servicesetting/"
                    "ssm/managed-instance/default-ec2-instance-management-role"):
            cloud = FakeCloud()
            cloud.api["ssm"].get_service_setting.return_value["ServiceSetting"]["ARN"] = arn
            with self.subTest(arn=arn), self.assertRaisesRegex(ValueError, "Host Management"):
                operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
            cloud.api["autoscaling"].suspend_processes.assert_not_called()
            cloud.api["s3"].put_object.assert_not_called()

    def test_wrong_startup_binding_or_numeric_version_fails_before_mutations(self):
        for change in ("direct", "durable", "unknown", "bucket", "region", "missing", "version", "duplicate"):
            cloud = FakeCloud()
            versions = cloud.api["ec2"].describe_launch_template_versions.return_value["LaunchTemplateVersions"]
            data = versions[0]["LaunchTemplateData"]
            if change in {"direct", "durable", "unknown"}:
                data["UserData"] = startup(change)
            elif change == "bucket":
                data["UserData"] = startup(bucket="other")
            elif change == "region":
                data["UserData"] = startup(region="us-west-2")
            elif change == "missing":
                del data["UserData"]
            elif change == "version":
                versions[0]["VersionNumber"] = 2
            else:
                versions.append(versions[0])
            with self.subTest(change=change), self.assertRaises(ValueError):
                operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
            cloud.api["autoscaling"].suspend_processes.assert_not_called()
            cloud.api["autoscaling"].put_scheduled_update_group_action.assert_not_called()
            cloud.api["s3"].put_object.assert_not_called()


if __name__ == "__main__":
    unittest.main()
