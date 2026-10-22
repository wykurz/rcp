"""Offline exact production guest startup and argument-boundary regressions."""

import base64
import shlex
import unittest

from infra.aws_benchmark import production_startup as startup

PINS = dict(account="123456789012", bucket="production-artifacts", region="us-east-1")


def user_data(script):
    return {"UserData": base64.b64encode(script.encode()).decode("ascii")}


class ProductionStartupTests(unittest.TestCase):
    def test_fixed_direct_command_selects_instance_mailbox_and_bounded_systemd_supervision(self):
        script = startup.reviewed_startup(**PINS)
        self.assertEqual(script.splitlines()[:2], ["#!/bin/bash", "set -euo pipefail"])
        command = shlex.split(script.splitlines()[-1])
        self.assertEqual(command[0], "systemd-run")
        self.assertIn("--unit=rcp-supervisor", command)
        self.assertIn("--working-directory=/opt/rcp-benchmark", command)
        properties = {part.removeprefix("--property=") for part in command if part.startswith("--property=")}
        self.assertEqual(properties, {"RuntimeMaxSec=10800", "TimeoutStopSec=10", "KillMode=control-group",
                                      "SendSIGKILL=yes", "FailureAction=poweroff", "SuccessAction=poweroff"})
        executable = command.index("/opt/rcp-benchmark/venv/bin/python")
        self.assertEqual(command[executable:], ["/opt/rcp-benchmark/venv/bin/python", "-m",
            "infra.aws_benchmark.worker", "--direct", "--production-account", PINS["account"],
            "--bucket", PINS["bucket"], "--region", PINS["region"], "||", "systemctl", "poweroff"])
        self.assertEqual(startup.verify_startup(user_data(script), **PINS), script)

    def test_binding_values_are_validated_before_shell_interpolation(self):
        invalid = {
            "account": [None, True, 123456789012, "12345678901", "1234567890123", "１２３４５６７８９０１２", "123456789012\n"],
            "region": [None, "us-east-0", "cn-north-1", "us-gov-west-1", "us-east-1'", "$(id)", "us-east-1\n"],
            "bucket": [None, "ab", "a" * 64, "with.dots", "UPPER", "-bucket", "bucket-", "a/b", "a' b", "a$()b"],
        }
        for key, values in invalid.items():
            for value in values:
                with self.subTest(key=key, value=value), self.assertRaisesRegex(ValueError, key):
                    startup.reviewed_startup(**{**PINS, key: value})
        for change in (dict(region="eu-west-1"), dict(bucket="a" * 63), dict(account="000000000000")):
            pins = {**PINS, **change}
            script = startup.reviewed_startup(**pins)
            self.assertEqual(startup.verify_startup(user_data(script), **pins), script)

    def test_legacy_mailbox_or_any_startup_and_binding_drift_is_rejected(self):
        script = startup.reviewed_startup(**PINS)
        mutations = [script.replace("--production-account '123456789012' ", ""),
            script.replace("--direct ", ""), script.replace("RuntimeMaxSec=10800", "RuntimeMaxSec=10801"),
            script.replace("KillMode=control-group", "KillMode=process"),
            script.replace("--property=FailureAction=poweroff ", ""),
            script.replace("--property=SuccessAction=poweroff ", ""),
            script.replace("SendSIGKILL=yes", "SendSIGKILL=no"),
            script.replace("|| systemctl poweroff", "|| true"), script + "echo changed\n"]
        for changed in mutations:
            with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, "exact reviewed production"):
                startup.verify_startup(user_data(changed), **PINS)
        for change in (dict(account="123456789013"), dict(region="eu-west-1"), dict(bucket="another-bucket")):
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "exact reviewed production"):
                startup.verify_startup(user_data(script), **{**PINS, **change})

    def test_missing_malformed_and_bounded_user_data_fail_closed(self):
        malformed = [None, {}, {"UserData": None}, {"UserData": b"YQ=="}, {"UserData": ""},
            {"UserData": "***"}, {"UserData": "YQ==\n"}, {"UserData": "☃"},
            {"UserData": base64.b64encode(b"\xff").decode("ascii")},
            {"UserData": "A" * (startup.MAX_ENCODED_USER_DATA + 1)},
            {"UserData": base64.b64encode(b"a" * (startup.MAX_USER_DATA_BYTES + 1)).decode("ascii")}]
        for data in malformed:
            with self.subTest(data_type=type(data).__name__), self.assertRaises(ValueError):
                startup.verify_startup(data, **PINS)


if __name__ == "__main__":
    unittest.main()
