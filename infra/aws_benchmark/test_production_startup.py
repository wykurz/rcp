"""Offline exact production guest startup and argument-boundary regressions."""

import base64
import hashlib
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
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
            script.replace("|| systemctl poweroff", "|| true"),
            script.replace(startup.OUTPUT_CONTRACT, "older-contract"),
            script.replace(startup.OUTPUT_WORKER_SHA256, "0" * 64), script + "echo changed\n"]
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


class InstalledOutputContractTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "image"
        self.worker = self.root / startup.OUTPUT_WORKER_FILE
        self.worker.parent.mkdir(parents=True)
        shutil.copyfile(Path(startup.__file__).with_name("worker.py"), self.worker)
        self.worker.chmod(0o644)
        self.marker = self.root / startup.OUTPUT_CONTRACT_FILE

    def test_contract_binds_reviewed_worker_bytes_and_read_only_installed_marker(self):
        self.assertEqual(hashlib.sha256(self.worker.read_bytes()).hexdigest(), startup.OUTPUT_WORKER_SHA256)
        startup.install_output_contract(self.root)
        self.assertEqual(self.marker.read_text(), startup.OUTPUT_CONTRACT + "\n")
        self.assertEqual(self.marker.stat().st_mode & 0o777, 0o444)

    def test_installer_rejects_changed_or_symlink_worker_without_claiming_compatibility(self):
        self.worker.write_text("older upload implementation")
        with self.assertRaisesRegex(ValueError, "reviewed production output contract"):
            startup.install_output_contract(self.root)
        self.assertFalse(self.marker.exists())
        self.worker.unlink()
        self.worker.symlink_to(Path(startup.__file__).with_name("worker.py"))
        with self.assertRaisesRegex(ValueError, "reviewed production output contract"):
            startup.install_output_contract(self.root)
        self.assertFalse(self.marker.exists())

    def test_installer_never_overwrites_an_existing_or_symlink_marker(self):
        self.marker.write_text("existing operator data")
        with self.assertRaises(FileExistsError):
            startup.install_output_contract(self.root)
        self.assertEqual(self.marker.read_text(), "existing operator data")
        self.marker.unlink()
        self.marker.symlink_to(self.worker)
        before = self.worker.read_bytes()
        with self.assertRaises(FileExistsError):
            startup.install_output_contract(self.root)
        self.assertEqual(self.worker.read_bytes(), before)

    def run_startup(self, *, owner="0:0"):
        commands = Path(self.directory.name) / "commands"
        commands.write_text("")
        bindir = Path(self.directory.name) / "bin"
        bindir.mkdir(exist_ok=True)
        for name in ("systemd-run", "systemctl"):
            script = bindir / name
            script.write_text("#!/bin/bash\nprintf '%s\\n' '" + name + "' >>\"$COMMAND_LOG\"\n")
            script.chmod(0o755)
        # only ownership is simulated; real stat modes, marker contents and sha256sum are exercised
        stat = bindir / "stat"
        stat.write_text("#!/bin/bash\nprintf '%s:%s\\n' \"$FAKE_OWNER\" \"$(/usr/bin/stat -c %a \"$3\")\"\n")
        stat.chmod(0o755)
        script = startup.reviewed_startup(**PINS).replace("/opt/rcp-benchmark", str(self.root))
        result = subprocess.run(["bash"], input=script, text=True, capture_output=True, timeout=5,
            env={**os.environ, "PATH": str(bindir) + os.pathsep + os.environ["PATH"],
                 "COMMAND_LOG": str(commands), "FAKE_OWNER": owner})
        return result, commands.read_text().splitlines()

    def test_compatible_image_reaches_supervisor_without_poweroff(self):
        startup.install_output_contract(self.root)
        result, commands = self.run_startup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(commands, ["systemd-run"])

    def test_missing_wrong_writable_and_foreign_marker_never_starts_workload(self):
        for fault in ("missing", "wrong", "writable", "foreign", "symlink"):
            with self.subTest(fault=fault):
                if self.marker.exists() or self.marker.is_symlink():
                    self.marker.unlink()
                if fault != "missing":
                    startup.install_output_contract(self.root)
                if fault == "wrong":
                    self.marker.chmod(0o644)
                    self.marker.write_text("old-contract\n")
                    self.marker.chmod(0o444)
                elif fault == "writable":
                    self.marker.chmod(0o666)
                elif fault == "symlink":
                    self.marker.unlink()
                    self.marker.symlink_to(self.worker)
                result, commands = self.run_startup(owner="1000:1000" if fault == "foreign" else "0:0")
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(commands, ["systemctl"])
                self.assertIn("incompatible production worker image", result.stderr)

    def test_copied_marker_cannot_approve_old_changed_missing_or_writable_worker(self):
        startup.install_output_contract(self.root)
        original = self.worker.read_bytes()
        for fault in ("changed", "missing", "writable", "symlink"):
            with self.subTest(fault=fault):
                if self.worker.exists() or self.worker.is_symlink():
                    self.worker.unlink()
                if fault == "symlink":
                    self.worker.symlink_to(Path(startup.__file__).with_name("worker.py"))
                elif fault != "missing":
                    self.worker.write_bytes(b"old worker" if fault == "changed" else original)
                    self.worker.chmod(0o666 if fault == "writable" else 0o644)
                result, commands = self.run_startup()
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(commands, ["systemctl"])


if __name__ == "__main__":
    unittest.main()
