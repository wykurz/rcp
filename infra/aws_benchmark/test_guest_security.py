"""Guest security tests use fake commands and a temporary filesystem, never host services."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from infra.aws_benchmark import guest_security as security


class FakeSystem:
    def __init__(self, root):
        self.root = root
        self.calls = []
        self.masked = True
        self.active = False
        self.deb = False
        self.fail = None
        self.states = {}

    def __call__(self, argv, **options):
        self.calls.append((argv, options))
        if self.fail and argv[:len(self.fail)] == self.fail:
            raise subprocess.TimeoutExpired(argv, options["timeout"])
        if argv[:2] == ["systemctl", "show"]:
            blocks = []
            for name in security.UNITS:
                fields = dict(MainPID="1" if self.active else "0", Id=name,
                              LoadState="masked" if self.masked else "loaded",
                              ActiveState="active" if self.active else "inactive",
                              UnitFileState="masked" if self.masked else "enabled")
                fields.update(self.states.get(name, {}))
                blocks.append("\n".join(k + "=" + v for k, v in fields.items()))
            output = "\n\n".join(blocks) + "\n"
        elif argv[:2] == ["systemctl", "stop"]:
            self.active = False
            output = ""
        elif argv[:2] == ["systemctl", "mask"]:
            self.masked = True
            output = ""
        elif argv[0] == "dpkg-query":
            output = "bash\tinstalled\n" + ("amazon-ssm-agent\tinstalled\n" if self.deb else "")
        elif argv == ["snap", "remove", "--purge", "amazon-ssm-agent"]:
            for name in ["snap/amazon-ssm-agent", "var/snap/amazon-ssm-agent"]:
                path = self.root / name
                if path.exists():
                    shutil.rmtree(path)
            for path in (self.root / "var/lib/snapd/snaps").glob("amazon-ssm-agent_*.snap"):
                path.unlink()
            output = ""
        elif argv == ["dpkg", "--purge", "amazon-ssm-agent"]:
            self.deb = False
            output = ""
        else:
            raise AssertionError("unexpected command: " + str(argv))
        return subprocess.CompletedProcess(argv, 0, output, "")


class GuestSecurityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.system = FakeSystem(self.root)

    def check(self, **kwargs):
        return security.check_ssm_absent(runner=self.system, root=self.root, **kwargs)

    def test_verification_is_read_only_and_bounded(self):
        result = self.check(timeout=lambda maximum: min(maximum, 4))
        self.assertEqual(result, {"ssm_agent": "absent", "services": "masked/inactive"})
        self.assertEqual([argv[:2] for argv, _ in self.system.calls],
                         [["systemctl", "show"], ["dpkg-query", "-W"]])
        for _, options in self.system.calls:
            self.assertEqual(options["timeout"], 4)
            self.assertTrue(options["check"])
            self.assertEqual(options["env"]["LC_ALL"], "C")

    def test_active_unmasked_or_missing_service_is_rejected(self):
        for name in security.UNITS:
            for change in [{"ActiveState": "active"}, {"UnitFileState": "enabled"},
                           {"UnitFileState": "masked-runtime"}, {"LoadState": "not-found"},
                           {"MainPID": "1"}, {"Id": "unexpected.service"}]:
                with self.subTest(name=name, change=change):
                    self.system.states = {name: change}
                    with self.assertRaises(ValueError):
                        self.check()

    def test_package_binary_and_registration_remnants_are_rejected(self):
        self.system.deb = True
        with self.assertRaisesRegex(ValueError, "package remains"):
            self.check()
        self.system.deb = False
        for relative in security.AGENT_PATHS + security.STATE_PATHS:
            with self.subTest(path=relative):
                path = self.root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
                with self.assertRaises(ValueError):
                    self.check()
                path.unlink()
        path = self.root / "var/lib/snapd/snaps/amazon-ssm-agent_123.snap"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        with self.assertRaisesRegex(ValueError, "package remains"):
            self.check()

    def test_dangling_agent_link_is_rejected(self):
        path = self.root / "usr/bin/amazon-ssm-agent"
        path.parent.mkdir(parents=True)
        path.symlink_to(self.root / "missing")
        with self.assertRaisesRegex(ValueError, "path remains"):
            self.check()

    def test_snap_removal_stops_then_purges_before_mask_and_sanitizes(self):
        self.system.masked, self.system.active = False, True
        for relative in ["snap/amazon-ssm-agent", "var/snap/amazon-ssm-agent", *security.STATE_PATHS]:
            path = self.root / relative
            path.mkdir(parents=True, exist_ok=True)
            (path / "registration").write_text("test fixture only")
        security.remove_ssm(runner=self.system, root=self.root)
        calls = [argv for argv, _ in self.system.calls]
        stop = calls.index(["systemctl", "stop", *security.UNITS])
        purge = calls.index(["snap", "remove", "--purge", "amazon-ssm-agent"])
        mask = calls.index(["systemctl", "mask", *security.UNITS])
        self.assertLess(stop, purge)
        self.assertLess(purge, mask)
        self.assertTrue(all(not (self.root / p).exists() for p in security.STATE_PATHS))
        self.assertEqual(self.check()["ssm_agent"], "absent")

    def test_deb_removal_does_not_invoke_apt_or_snap(self):
        self.system.deb = True
        security.remove_ssm(runner=self.system, root=self.root)
        calls = [argv for argv, _ in self.system.calls]
        self.assertIn(["dpkg", "--purge", "amazon-ssm-agent"], calls)
        self.assertFalse(any(argv[0] in {"snap", "apt", "apt-get"} for argv in calls))

    def test_missing_units_can_be_masked_without_stopping_unknown_services(self):
        original = self.system
        def runner(argv, **kwargs):
            if argv[:2] == ["systemctl", "show"] and not original.calls:
                original.states = {name: {"LoadState": "not-found", "UnitFileState": ""}
                                   for name in security.UNITS}
            else:
                original.states = {}
            return original(argv, **kwargs)
        security.remove_ssm(runner=runner, root=self.root)
        self.assertFalse(any(argv[:2] == ["systemctl", "stop"] for argv, _ in self.system.calls))

    def test_removal_timeout_cannot_report_verified_absence(self):
        (self.root / "snap/amazon-ssm-agent").mkdir(parents=True)
        self.system.fail = ["snap", "remove"]
        with self.assertRaises(subprocess.TimeoutExpired):
            security.remove_ssm(runner=self.system, root=self.root)
        self.assertFalse(any(argv[:2] == ["systemctl", "mask"] for argv, _ in self.system.calls))

    def test_budget_exhaustion_stops_before_any_read(self):
        with self.assertRaises(TimeoutError):
            self.check(timeout=lambda maximum: 0)
        self.assertEqual(self.system.calls, [])

    def test_removal_total_budget_is_not_reset_between_commands(self):
        ticks = iter([0, 0, 10, 301])
        with self.assertRaisesRegex(TimeoutError, "five minutes"):
            security.remove_ssm(runner=self.system, root=self.root, clock=lambda: next(ticks))


if __name__ == "__main__":
    unittest.main()
