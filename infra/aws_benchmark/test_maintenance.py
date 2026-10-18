"""Offline APT isolation tests; all systemctl operations are fake."""

import subprocess
import unittest
from unittest.mock import patch

from infra.aws_benchmark import maintenance as apt


class FakeSystemd:
    def __init__(self, activity="inactive"):
        self.activity = activity
        self.timer_activity = "inactive"
        self.stop_effective = True
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
        if argv[1] == "stop" and self.stop_effective and self.timer_activity != "failed":
            self.timer_activity = "inactive"
        if argv[1] == "mask" and self.timer_activity == "active":
            # systemd 255 reloads a masked waiting timer before --now tries to stop it
            self.timer_activity = "failed"
        if argv[1] != "show":
            return ""
        activity = "inactive" if self.finish_at is not None and self.clock >= self.finish_at else self.activity
        return "\n\n".join("Id=" + name + "\nUnitFileState=" + self.mask + "\nActiveState="
                           + (activity if name in apt.SERVICES else self.timer_activity)
                           for name in self.units) + "\n"


class MaintenanceTests(unittest.TestCase):
    def test_masks_persist_and_active_services_drain_without_being_stopped(self):
        for activity in ("active", "activating", "deactivating"):
            with self.subTest(activity=activity):
                fake = FakeSystemd(activity)
                fake.timer_activity = "active"
                fake.finish_at = 4
                result = apt.quiesce(fake, now=lambda: fake.clock, sleep=fake.sleep)
                self.assertEqual(fake.calls[0][0], ["systemctl", "stop", *apt.TIMERS])
                self.assertEqual(fake.calls[1][0], ["systemctl", "mask", *apt.UNITS])
                self.assertTrue(all(c[0][1] == "show" for c in fake.calls[2:]))
                self.assertEqual(fake.clock, 4)
                self.assertTrue(all(s["ActiveState"] == "inactive" for s in result.values()))
                self.assertTrue(all(0 < timeout <= 10 for _, timeout in fake.calls))

    def test_active_work_times_out_without_killing_or_restarting_services(self):
        fake = FakeSystemd("active")
        with self.assertRaisesRegex(TimeoutError, "did not drain"):
            apt.quiesce(fake, now=lambda: fake.clock, sleep=fake.sleep)
        self.assertEqual(fake.clock, apt.DRAIN_SECONDS)
        self.assertEqual([argv for argv, _ in fake.calls if argv[1] == "stop"],
                         [["systemctl", "stop", *apt.TIMERS]])
        self.assertFalse(any(argv[1] in {"restart", "reset-failed"} for argv, _ in fake.calls))

    def test_mask_now_reproduction_preserves_failed_timer_despite_successful_command(self):
        fake = FakeSystemd()
        fake.timer_activity = "active"
        fake(["systemctl", "mask", "--now", *apt.TIMERS], timeout=10)
        with self.assertRaisesRegex(ValueError, "apt-daily.timer=failed"):
            apt.check_quiet(fake)

    def test_failed_or_unstopped_timer_is_rejected_without_resetting_failure(self):
        for state in ("active", "failed", "deactivating", "unknown"):
            with self.subTest(state=state):
                fake = FakeSystemd()
                fake.timer_activity = state
                fake.stop_effective = False
                with self.assertRaisesRegex(ValueError, "automatic APT timer did not stop: apt-daily.timer="):
                    apt.quiesce(fake, now=lambda: fake.clock, sleep=fake.sleep)
                self.assertEqual(len(fake.calls), 3)
                self.assertFalse(any(argv[1] == "reset-failed" for argv, _ in fake.calls))

    def test_stop_timeout_prevents_masking_and_propagates(self):
        with patch.object(apt.subprocess, "run", side_effect=subprocess.TimeoutExpired("systemctl", 10)) as run:
            with self.assertRaises(subprocess.TimeoutExpired):
                apt.quiesce()
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.args[0], ["systemctl", "stop", *apt.TIMERS])

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
        for operation in ("stop", "mask", "show"):
            with self.subTest(operation=operation):
                fake = FakeSystemd()
                fake.fail = operation
                with self.assertRaises(subprocess.CalledProcessError):
                    apt.quiesce(fake, now=lambda: fake.clock, sleep=fake.sleep)
                if operation == "stop":
                    self.assertEqual(len(fake.calls), 1)

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
