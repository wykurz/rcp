"""Runner invariants around optional command clock diagnostics."""

import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import unittest
from unittest import mock

from benchmarks import clocks, report, run, sanitized
from benchmarks.test_command_clocks import sample
from benchmarks.test_report import sample_run


class ClockRunnerContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_start_sample_cancellation_prevents_child_launch(self):
        for index, error in enumerate((InterruptedError("SIGTERM"), KeyboardInterrupt())):
            with self.subTest(error=type(error).__name__), mock.patch.object(clocks.time, "monotonic_ns", side_effect=error), mock.patch.object(run.subprocess, "Popen") as launch:
                with self.assertRaises(type(error)):
                    run.execute_commands([["must-not-launch"]], self.root / str(index), 1, command_clocks=True)
                launch.assert_not_called()

    def test_start_sampling_cost_is_outside_elapsed_and_timeout(self):
        now = 0
        observations = 0
        waiting = []
        condition = threading.Condition()
        class Child:
            pid = 1000000
            returncode = None
            def wait(self):
                nonlocal now
                now += .01
                self.returncode = 0
                return 0
        class Waiter:
            def __init__(self, *, target, daemon):
                self.target = target
            def start(self):
                waiting.append(self)
            def join(self):
                if self in waiting:
                    waiting.remove(self)
                    self.target()
        def observe():
            nonlocal observations, now
            observations += 1
            if observations == 1:
                now = 10
                return sample()
            return sample(200, 220, 1130, 10070)
        def complete(_timeout):
            for waiter in tuple(waiting):
                waiter.join()
        with mock.patch.object(run.subprocess, "Popen", return_value=Child()), mock.patch.object(clocks, "sample", side_effect=observe), mock.patch.object(run.time, "monotonic", side_effect=lambda: now), mock.patch.object(run.threading, "Thread", Waiter), mock.patch.object(run.threading, "Condition", return_value=condition), mock.patch.object(condition, "wait", side_effect=complete), mock.patch.object(run.os, "killpg", side_effect=ProcessLookupError) as kill:
            outcome = run.execute_commands([["controlled-child"]], self.root / "start-cost", .05, command_clocks=True)
        self.assertTrue(outcome["ok"])
        self.assertFalse(outcome["timed_out"])
        self.assertAlmostEqual(outcome["elapsed_seconds"], .01)
        self.assertEqual(observations, 2)
        kill.assert_not_called()

    def test_waiter_sampling_failure_preserves_outcome_and_importability(self):
        main_thread = threading.current_thread()
        def observe():
            if threading.current_thread() is not main_thread:
                raise MemoryError("synthetic sampling failure")
            return sample()
        for code in (0, 3):
            with self.subTest(code=code), mock.patch.object(clocks, "sample", side_effect=observe), mock.patch.object(threading, "excepthook") as unhandled:
                outcome = run.execute_commands([[sys.executable, "-c", f"raise SystemExit({code})"]], self.root / f"waiter-{code}", 5, command_clocks=True)
            unhandled.assert_not_called()
            self.assertEqual(outcome["ok"], code == 0)
            self.assertEqual(outcome["exit_codes"], [code])
            self.assertFalse(outcome["timed_out"])
            value = outcome["command_clocks"]
            self.assertEqual(value["finish_command_index"], 0)
            self.assertEqual(value["finish"], {key: dict(unavailable="read_failed") for key in clocks.READINGS})
            clocks.validate_observation(value, outcome["exit_codes"])
            record = sample_run()
            record["context"].update(topology="local", command_clocks=clocks.POLICY, measurement_environment={})
            for row in record["trials"]:
                row.update({key: value for key, value in outcome.items() if key != "elapsed_seconds"}, status="ok" if code == 0 else "failed", validation=dict(ok=code == 0))
            if code:
                record.update(status="failed", summaries=[])
            report.validate_result(record)
            projected = sanitized.project_run(record, [])
            self.assertEqual(projected["trials"][0]["command_clocks"]["status"], "monotonic_unavailable")
            self.assertNotIn("synthetic sampling failure", json.dumps(record))

    def test_main_thread_cancellation_during_waiter_sample_still_propagates(self):
        main_thread = threading.current_thread()
        original_handler = signal.getsignal(signal.SIGTERM)
        def terminate(_number, _frame):
            raise InterruptedError("SIGTERM")
        def observe():
            if threading.current_thread() is not main_thread:
                os.kill(os.getpid(), signal.SIGTERM)
                raise MemoryError("synthetic sampling failure")
            return sample()
        signal.signal(signal.SIGTERM, terminate)
        try:
            with mock.patch.object(clocks, "sample", side_effect=observe), self.assertRaises(InterruptedError):
                run.execute_commands([[sys.executable, "-c", "pass"]], self.root / "cancelled", 5, command_clocks=True)
        finally:
            signal.signal(signal.SIGTERM, original_handler)

    def test_clock_free_early_failures_keep_environment_absent(self):
        cases = [
            ["--repetitions", "0"],
            ["--local-resources", "--repetitions", "0"],
            ["--build-provenance", str(self.root / "not-probed.json"), "--timeout", "0"],
            ["--paired-seed", "1", "--repetitions", "2"],
            ["--paired-seed", "1", "--no-timings", "--repetitions", "3"],
            ["--paired-seed", "-1", "--no-timings", "--repetitions", "2"],
        ]
        for index, args in enumerate(cases):
            output = self.root / f"preflight-{index}"
            with self.subTest(args=args), mock.patch.dict(os.environ, {"RUST_LOG": "synthetic-private-value"}), mock.patch.object(run, "_tool") as probe, self.assertRaises(ValueError):
                run.main([*args, "--output", str(output)])
            probe.assert_not_called()
            record = report.parse_result((output / "results.json").read_text())
            self.assertNotIn("measurement_environment", record["context"])
            self.assertNotIn("command_clocks", record["context"])
            projected = sanitized.project_run(record, [])
            self.assertNotIn("measurement_environment", projected)
            self.assertNotIn("synthetic-private-value", json.dumps(record))

    def test_opted_in_early_failures_keep_environment_importable(self):
        output = self.root / "clock-preflight"
        with mock.patch.dict(os.environ, {"RUST_LOG": "synthetic-value"}), mock.patch.object(run, "_tool") as probe, self.assertRaises(ValueError):
            run.main(["--command-clocks", "--local-resources", "--repetitions", "0", "--output", str(output)])
        probe.assert_not_called()
        record = report.parse_result((output / "results.json").read_text())
        self.assertEqual(record["context"]["measurement_environment"]["RUST_LOG"], "synthetic-value")
        self.assertIn("command_clocks", sanitized.project_run(record, []))

    def test_summary_distinguishes_absent_bounds_from_unavailable_readings(self):
        record = sample_run()
        record["context"].update(topology="local", command_clocks=clocks.POLICY, measurement_environment={})
        missing = sample(200, 220, 1130, 10070)
        missing.update(raw=dict(unavailable="unsupported"), realtime=dict(unavailable="read_failed"))
        for row, finish, index in zip(record["trials"], (sample(105, 115, 1130, 10070), missing, None), (0, 0, None)):
            row["command_clocks"] = clocks.observation(sample(), finish, index)
        record.update(status="failed", summaries=[])
        record["trials"][-1].update(exit_codes=[], status="failed", validation=dict(ok=False))
        report.validate_result(record)
        run._persist(self.root, record)
        text = (self.root / "summary.md").read_text()
        self.assertIn("| monotonic_regressed | no bounds | no bounds |", text)
        self.assertIn("| bounded | unavailable | unavailable |", text)
        self.assertIn("| no_completed_child | no bounds | no bounds |", text)


if __name__ == "__main__":
    unittest.main()
