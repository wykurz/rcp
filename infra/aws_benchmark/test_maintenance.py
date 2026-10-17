"""Offline APT isolation tests; all systemctl operations are fake."""

import subprocess
import unittest
from unittest.mock import patch

from infra.aws_benchmark import maintenance as apt


class FakeSystemd:
    def __init__(self, activity="inactive"):
        self.activity = activity
        self.mask = "masked"
        self.clock = 0
        self.calls = []
        self.finish_at = None
        self.fail = None
        self.units = apt.UNITS

    def sleep(self, seconds):
        self.clock += seconds

    def __call__(self, argv, *, timeout):
        self.calls.append((argv, timeout))
        if argv[1] == self.fail:
            raise subprocess.CalledProcessError(1, argv)
        if argv[1] != "show":
            return ""
        activity = "inactive" if self.finish_at is not None and self.clock >= self.finish_at else self.activity
        return "\n\n".join("Id=" + name + "\nUnitFileState=" + self.mask + "\nActiveState="
                           + (activity if name in apt.SERVICES else "inactive") for name in self.units) + "\n"


class MaintenanceTests(unittest.TestCase):
    def test_masks_persist_and_active_services_drain_without_being_stopped(self):
        for activity in ("active", "activating", "deactivating"):
            with self.subTest(activity=activity):
                fake = FakeSystemd(activity)
                fake.finish_at = 4
                result = apt.quiesce(fake, now=lambda: fake.clock, sleep=fake.sleep)
                self.assertEqual(fake.calls[0][0], ["systemctl", "mask", "--now", *apt.TIMERS])
                self.assertEqual(fake.calls[1][0], ["systemctl", "mask", *apt.SERVICES])
                self.assertTrue(all(c[0][1] == "show" for c in fake.calls[2:]))
                self.assertEqual(fake.clock, 4)
                self.assertTrue(all(s["ActiveState"] == "inactive" for s in result.values()))
                self.assertTrue(all(0 < timeout <= 10 for _, timeout in fake.calls))

    def test_active_work_times_out_without_killing_or_restarting_services(self):
        fake = FakeSystemd("active")
        with self.assertRaisesRegex(TimeoutError, "did not drain"):
            apt.quiesce(fake, now=lambda: fake.clock, sleep=fake.sleep)
        self.assertEqual(fake.clock, apt.DRAIN_SECONDS)
        self.assertFalse(any("stop" in argv or "restart" in argv for argv, _ in fake.calls))

    def test_missing_runtime_only_or_missing_units_fail_closed(self):
        for mask, units in (("enabled", apt.UNITS), ("masked-runtime", apt.UNITS),
                            ("masked", apt.UNITS[:-1]), ("masked", apt.UNITS + apt.UNITS[:1])):
            with self.subTest(mask=mask, units=units):
                fake = FakeSystemd()
                fake.mask, fake.units = mask, units
                with self.assertRaises(ValueError):
                    apt.check_quiet(fake)

    def test_worker_check_is_read_only_and_rejects_active_or_failed_services(self):
        for activity in ("active", "activating", "failed"):
            with self.subTest(activity=activity):
                fake = FakeSystemd(activity)
                with self.assertRaisesRegex(ValueError, "not inactive"):
                    apt.check_quiet(fake)
                self.assertEqual(len(fake.calls), 1)
                self.assertEqual(fake.calls[0][0][1], "show")

    def test_command_failure_never_counts_as_inactivity(self):
        for operation in ("mask", "show"):
            with self.subTest(operation=operation):
                fake = FakeSystemd()
                fake.fail = operation
                with self.assertRaises(subprocess.CalledProcessError):
                    apt.quiesce(fake, now=lambda: fake.clock, sleep=fake.sleep)

    def test_failed_service_is_not_reset_to_hide_the_failure(self):
        fake = FakeSystemd("failed")
        with self.assertRaisesRegex(ValueError, "unexpected APT service"):
            apt.quiesce(fake, now=lambda: fake.clock, sleep=fake.sleep)
        self.assertEqual(len(fake.calls), 3)

    def test_systemctl_timeout_is_enforced_and_propagated(self):
        with patch.object(apt.subprocess, "run", side_effect=subprocess.TimeoutExpired("systemctl", 10)) as run:
            with self.assertRaises(subprocess.TimeoutExpired):
                apt.check_quiet()
        self.assertEqual(run.call_args.kwargs["timeout"], 10)
        self.assertTrue(run.call_args.kwargs["check"])


if __name__ == "__main__":
    unittest.main()
