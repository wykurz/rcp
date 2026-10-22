"""Offline reviewed startup and exact numeric launch-template validation."""

import base64
import copy
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

from infra.aws_benchmark import ami_image
from infra.aws_benchmark.common import retry_io


class ReviewedStartupTests(unittest.TestCase):
    def template(self, script):
        return json.dumps({"Resources": {"LaunchTemplate": {"Properties": {"LaunchTemplateData": {
            "UserData": {"Fn::Base64": {"Fn::Sub": script}}}}}}})

    def test_explicit_substitutions_preserve_unbraced_shell_variables(self):
        script = "${WorkerProtocol}|${Artifacts}|${AWS::Region}|$mode|$runtime"
        with patch.object(ami_image.Path, "read_text", return_value=self.template(script)):
            self.assertEqual(ami_image.reviewed_startup("direct", "my-bucket", "us-east-1"),
                             "direct|my-bucket|us-east-1|$mode|$runtime")

    def test_new_or_malformed_placeholders_fail_before_comparing_cloud_userdata(self):
        for placeholder in ("${AWS::StackName}", "${Unknown}", "${!mode}", "${}", "${", "${Artifacts"):
            with self.subTest(placeholder=placeholder), patch.object(
                    ami_image.Path, "read_text", return_value=self.template("echo " + placeholder)):
                with self.assertRaisesRegex(ValueError, "placeholder"):
                    ami_image.reviewed_startup("direct", "bucket", "us-east-1")

    def test_unknown_protocol_and_substitution_injection_are_rejected(self):
        for protocol, bucket, region in (([], "bucket", "us-east-1"), ("durable", "bucket", "us-east-1"),
                                        ("unknown", "bucket", "us-east-1"),
                                        ("direct", "${AWS::StackName}", "us-east-1"),
                                        ("direct", "bucket", None)):
            with self.subTest(protocol=protocol, bucket=bucket, region=region), self.assertRaises(ValueError):
                ami_image.reviewed_startup(protocol, bucket, region)

    def test_missing_or_changed_template_shape_is_rejected(self):
        for body in ({}, {"Resources": []}, json.loads(self.template({"name": "value"}))):
            with self.subTest(body=body), patch.object(ami_image.Path, "read_text", return_value=json.dumps(body)):
                with self.assertRaisesRegex(ValueError, "startup"):
                    ami_image.reviewed_startup("direct", "bucket", "us-east-1")

    def test_both_protocols_validate_actual_startup_and_reject_wrong_bindings(self):
        for protocol in ("operator-window", "direct"):
            script = ami_image.reviewed_startup(protocol, "bucket", "us-east-1")
            data = {"UserData": base64.b64encode(script.encode()).decode()}
            with self.subTest(protocol=protocol):
                self.assertEqual(ami_image.verify_startup(data, protocol, "bucket", "us-east-1"), script)
                for other, bucket, region in ((protocol, "other", "us-east-1"),
                                              (protocol, "bucket", "us-west-2"),
                                              ("direct" if protocol == "operator-window" else "operator-window",
                                               "bucket", "us-east-1")):
                    with self.assertRaisesRegex(ValueError, "exact reviewed"):
                        ami_image.verify_startup(data, other, bucket, region)

    def test_missing_nonstring_malformed_base64_and_non_utf8_are_validation_errors(self):
        for data in ({}, None, {"UserData": None}, {"UserData": b"YQ=="}, {"UserData": ""},
                     {"UserData": "***"}, {"UserData": "YQ==\n"}, {"UserData": "☃"},
                     {"UserData": base64.b64encode(b"\xff").decode()}):
            with self.subTest(data=data), self.assertRaisesRegex(ValueError, "UserData"):
                ami_image.verify_startup(data, "direct", "bucket", "us-east-1")


class NumericLaunchTemplateTests(unittest.TestCase):
    def setUp(self):
        self.ec2 = Mock()
        self.data = {"UserData": "YQ=="}
        self.response = {"LaunchTemplateVersions": [{
            "LaunchTemplateId": "lt-one", "VersionNumber": 9, "LaunchTemplateData": self.data}]}
        self.ec2.describe_launch_template_versions.return_value = self.response

    def test_exact_numeric_lookup_returns_data_through_callers_bounded_retry(self):
        self.ec2.describe_launch_template_versions.side_effect = [TimeoutError(), self.response]
        budget = Mock()
        budget.left.return_value = 30
        with patch("infra.aws_benchmark.common.time.sleep") as sleep:
            data = ami_image.launch_template_data(self.ec2, "lt-one", "9",
                read=lambda operation: retry_io(operation, budget))
        self.assertIs(data, self.data)
        self.assertEqual(self.ec2.describe_launch_template_versions.call_count, 2)
        self.ec2.describe_launch_template_versions.assert_called_with(LaunchTemplateId="lt-one", Versions=["9"])
        sleep.assert_called_once_with(1)
        self.assertGreaterEqual(budget.left.call_count, 2)

    def test_symbolic_nonnumeric_and_zero_versions_do_not_read_cloud(self):
        for version in ("$Latest", "$Default", "0", "09", "9.0", 9, True, None):
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "numeric"):
                ami_image.launch_template_data(self.ec2, "lt-one", version)
        self.ec2.describe_launch_template_versions.assert_not_called()

    def test_missing_duplicate_changed_identity_or_data_cannot_be_used(self):
        cases = [None, {}, {"LaunchTemplateVersions": []}, {"LaunchTemplateVersions": [None]},
                 {"LaunchTemplateVersions": self.response["LaunchTemplateVersions"] * 2}]
        for field, value in (("VersionNumber", 10), ("VersionNumber", True), ("VersionNumber", "9"),
                             ("LaunchTemplateId", "lt-other"), ("LaunchTemplateData", None)):
            response = copy.deepcopy(self.response)
            response["LaunchTemplateVersions"][0][field] = value
            cases.append(response)
        cases.append({**self.response, "NextToken": "more"})
        for response in cases:
            self.ec2.describe_launch_template_versions.return_value = response
            with self.subTest(response=response), self.assertRaises(ValueError):
                ami_image.launch_template_data(self.ec2, "lt-one", "9")

    def test_numeric_helper_import_does_not_require_operator_yaml_dependency(self):
        program = """
import builtins, sys
sys.path.insert(0, sys.argv[1])
original = builtins.__import__
def without_yaml(name, *args, **kwargs):
    if name == "yaml" or name.startswith("yaml."):
        raise ImportError("operator dependency excluded")
    return original(name, *args, **kwargs)
builtins.__import__ = without_yaml
from infra.aws_benchmark.ami_image import launch_template_data
class EC2:
    def describe_launch_template_versions(self, **kwargs):
        return {"LaunchTemplateVersions": [{"VersionNumber": 9, "LaunchTemplateData": {}}]}
assert launch_template_data(EC2(), "lt-one", "9") == {}
"""
        result = subprocess.run([sys.executable, "-I", "-c", program, str(Path(__file__).resolve().parents[2])],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
