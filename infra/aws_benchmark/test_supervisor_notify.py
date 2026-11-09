"""No real systemd calls: sender attribution, finite notification and image-test contracts."""

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from . import supervisor_notify as notify


@contextmanager
def fixture(*, changes=None, left=5000, clock=None, dynamic_library=False):
    values = {"Type": 's "notify"', "NotifyAccess": 's "main"', "MainPID": "u 1234",
              "RuntimeMaxUSec": "t 10800000000"}
    values.update(changes or {})
    budget = Mock()
    budget.left.return_value = left
    library = Mock()
    library.sd_notify.return_value = library.sd_notify_barrier.return_value = 1
    def read(argv, **kwargs):
        return SimpleNamespace(stdout=values[argv[-1]], returncode=0)
    with patch.dict(notify.os.environ, {"NOTIFY_SOCKET": "/run/systemd/notify"}), \
         patch.object(notify.os, "geteuid", return_value=0), \
         patch.object(notify.os, "getpid", return_value=1234), \
         patch.object(notify.subprocess, "run", side_effect=read) as run, \
         patch.object(notify.ctypes, "CDLL", return_value=library) as load:
        sender = notify.Notifier(budget, library=None if dynamic_library else library,
                                 monotonic=clock or (lambda: 1.0))
        yield sender, budget, library, run, load


class NotificationTests(unittest.TestCase):
    def test_typed_main_pid_and_exact_notify_authority_are_proven_before_ready(self):
        with fixture(dynamic_library=True) as (sender, budget, library, run, load):
            self.assertEqual([call.args[0][-1] for call in run.call_args_list],
                             ["Type", "NotifyAccess", "MainPID", "RuntimeMaxUSec"])
            self.assertTrue(all(call.kwargs["timeout"] == 2 for call in run.call_args_list))
            load.assert_called_once_with("libsystemd.so.0", use_errno=True)
            sender.ready(budget)
            library.sd_notify.assert_called_once_with(0, b"READY=1")
            library.sd_notify_barrier.assert_called_once_with(0, 1_000_000)
            self.assertTrue(sender.ready_sent)

    def test_unknown_wrong_or_untyped_unit_authority_is_rejected(self):
        for changes in ({"MainPID": "u 4321"}, {"MainPID": "1234"}, {"MainPID": "u 0"},
                        {"Type": 's "simple"'}, {"NotifyAccess": 's "all"'},
                        {"RuntimeMaxUSec": "t 28800000000"}, {"RuntimeMaxUSec": "3h"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                with fixture(changes=changes):
                    pass

    def test_child_pid_or_changed_socket_cannot_send_main_notifications(self):
        with fixture() as (sender, budget, library, _, _):
            with patch.object(notify.os, "getpid", return_value=4321), self.assertRaisesRegex(ValueError, "identity"):
                sender.ready(budget)
            library.sd_notify.assert_not_called()
        with fixture() as (sender, budget, library, _, _):
            with patch.dict(notify.os.environ, {"NOTIFY_SOCKET": "/wrong"}), self.assertRaises(ValueError):
                sender.ready(budget)
            library.sd_notify.assert_not_called()

    def test_zero_or_negative_send_and_barrier_are_failures_without_blind_retry(self):
        for method in ("sd_notify", "sd_notify_barrier"):
            for value in (0, -110):
                with self.subTest(method=method, value=value), fixture() as (sender, budget, library, _, _):
                    getattr(library, method).return_value = value
                    with self.assertRaises(ValueError):
                        sender.ready(budget)
                    with self.assertRaisesRegex(ValueError, "retried"):
                        sender.ready(budget)
                    self.assertFalse(sender.ready_sent)
                    self.assertEqual(library.sd_notify.call_count, 1)

    def test_extension_is_once_and_clipped_to_remaining_deadline_and_ack_bound(self):
        with fixture() as (sender, budget, library, _, _):
            with self.assertRaisesRegex(ValueError, "readiness"):
                sender.extend(budget, maximum_seconds=28800, shutdown_seconds=300)
            sender.ready(budget)
            value = sender.extend(budget, maximum_seconds=28800, shutdown_seconds=300)
            self.assertEqual(value, 5_298_000_000)
            self.assertEqual(library.sd_notify.call_args.args, (0, b"EXTEND_TIMEOUT_USEC=5298000000"))
            with self.assertRaises(ValueError):
                sender.extend(budget, maximum_seconds=28800, shutdown_seconds=300)
            self.assertEqual(library.sd_notify.call_count, 2)
        with fixture(left=40000) as (sender, budget, _, _, _):
            sender.ready(budget)
            self.assertEqual(sender.extend(budget, maximum_seconds=28800, shutdown_seconds=300),
                             28_798_000_000)

    def test_slow_or_failed_ack_never_permits_extension_replay(self):
        clock = [0.0]
        with fixture(clock=lambda: clock[0]) as (sender, budget, library, _, _):
            sender.ready(budget)
            def delayed(unset, state):
                clock[0] += 3
                return 1
            library.sd_notify.side_effect = delayed
            with self.assertRaisesRegex(ValueError, "bound"):
                sender.extend(budget, maximum_seconds=28800, shutdown_seconds=300)
            with self.assertRaisesRegex(ValueError, "readiness"):
                sender.extend(budget, maximum_seconds=28800, shutdown_seconds=300)
            self.assertEqual(library.sd_notify.call_count, 2)
        with fixture() as (sender, budget, library, _, _):
            sender.ready(budget)
            library.sd_notify_barrier.return_value = -110
            with self.assertRaises(ValueError):
                sender.extend(budget, maximum_seconds=28800, shutdown_seconds=300)
            self.assertTrue(sender.extension_attempted)

    def test_missing_context_library_or_finite_budget_fails_closed(self):
        with patch.dict(notify.os.environ, {}, clear=True), self.assertRaisesRegex(ValueError, "context"):
            notify.Notifier(Mock())
        with fixture() as (sender, budget, library, _, _):
            budget.left.return_value = 0
            with self.assertRaisesRegex(ValueError, "budget"):
                sender.ready(budget)
            library.sd_notify.assert_not_called()
        with fixture() as (_, budget, _, _, _):
            with patch.object(notify.ctypes, "CDLL", side_effect=OSError("missing")), self.assertRaises(OSError):
                notify.Notifier(budget)


class ImageAcceptanceTests(unittest.TestCase):
    def test_future_image_probe_is_one_private_non_poweroff_unit_and_scoped_cleanup(self):
        calls = []
        def run(argv, **kwargs):
            calls.append((argv, kwargs))
            if argv[0] == "/usr/bin/systemd-run":
                return SimpleNamespace(returncode=0, stdout=notify.SELF_TEST_MARKER + "\n")
            if "show" in argv:
                return SimpleNamespace(returncode=0, stdout="LoadState=not-found\nActiveState=inactive\nSubState=dead\n")
            return SimpleNamespace(returncode=5, stdout="")
        with patch.object(notify.os, "geteuid", return_value=0), \
             patch.object(notify.subprocess, "run", side_effect=run):
            result = notify.image_self_test()
        self.assertTrue(result["passed"])
        command, options = calls[0]
        self.assertEqual(options["timeout"], 10)
        self.assertIn("--property=RuntimeMaxSec=1s", command)
        self.assertIn("--property=Type=notify", command)
        self.assertIn("--property=NotifyAccess=main", command)
        self.assertIn("--property=PrivateNetwork=yes", command)
        self.assertIn("--property=FailureAction=none", command)
        self.assertIn("--property=SuccessAction=none", command)
        self.assertIn("infra.aws_benchmark.supervisor_notify", command)
        self.assertNotIn("__main__", command)
        unit = next(x.split("=", 1)[1] for x in command if x.startswith("--unit="))
        self.assertRegex(unit, r"^rcp-notify-selftest-[0-9a-f]{32}[.]service$")
        self.assertEqual([c[0][1] for c in calls[1:]], ["stop", "reset-failed", "show"])
        self.assertTrue(all(unit in c[0] and c[1]["timeout"] == 2 for c in calls[1:]))
        source = Path(__file__).with_name("install-worker.sh").read_text()
        self.assertIn("large_storage,supervisor_notify,cache_client", source)
        self.assertIn("--image-self-test > /opt/rcp-benchmark/supervisor-notify-acceptance.json", source)

    def test_failed_future_probe_still_cleans_only_its_exact_unit(self):
        calls = []
        def run(argv, **kwargs):
            calls.append(argv)
            if "show" in argv:
                return SimpleNamespace(returncode=0, stdout="LoadState=not-found\nActiveState=inactive\nSubState=dead\n")
            return SimpleNamespace(returncode=1, stdout="")
        with patch.object(notify.os, "geteuid", return_value=0), \
             patch.object(notify.subprocess, "run", side_effect=run), self.assertRaisesRegex(ValueError, "survive"):
            notify.image_self_test()
        self.assertEqual([c[1] for c in calls[1:]], ["stop", "reset-failed", "show"])


if __name__ == "__main__":
    unittest.main()
