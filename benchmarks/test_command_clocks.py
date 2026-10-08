"""Command clock qualification, import boundaries and completion ownership."""

import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from benchmarks import clocks, measurements, report, run, sanitized, test_experiment_boundaries, test_report
from benchmarks.test_report import sample_run


def reading(value):
    return dict(value_ns=value)


def sample(before=100, after=110, raw=1000, realtime=10000):
    return dict(monotonic_before=reading(before), raw=reading(raw), realtime=reading(realtime), monotonic_after=reading(after))


def observed_record():
    record = sample_run()
    record["context"].update(topology="local", command_clocks=clocks.POLICY, measurement_environment={})
    for row in record["trials"]:
        row["command_clocks"] = clocks.observation(sample(), sample(200, 220, 1130, 10070), 0)
    return record


class ClockQualificationTests(unittest.TestCase):
    def test_sequential_read_uncertainty_and_signed_clock_steps(self):
        value = clocks.compare(sample(), sample(200, 220, 1130, 9970))
        self.assertEqual(value["monotonic_elapsed_bounds_ns"], [90, 120])
        self.assertEqual(value["raw_elapsed_ns"], 130)
        self.assertEqual(value["raw_minus_monotonic_bounds_ns"], [10, 40])
        self.assertEqual(value["realtime_elapsed_ns"], -30)
        self.assertEqual(value["realtime_minus_monotonic_bounds_ns"], [-150, -120])
        self.assertEqual(value["start_read_skew_ns"], 10)
        self.assertEqual(value["finish_read_skew_ns"], 20)

    def test_bad_monotonic_brackets_retain_deltas_without_inventing_bounds(self):
        for start, finish, status in ((sample(110, 100), sample(200, 220), "monotonic_regressed"),
                                      (sample(), sample(200, 190), "monotonic_regressed"),
                                      (sample(), sample(80, 90), "monotonic_regressed"),
                                      (sample(), sample(105, 115), "monotonic_regressed"),
                                      (sample(), sample(90, 120), "monotonic_regressed")):
            with self.subTest(status=status, start=start, finish=finish):
                value = clocks.compare(start, finish)
                self.assertEqual(value["status"], status)
                self.assertIsNone(value["monotonic_elapsed_bounds_ns"])
                self.assertIsNone(value["raw_minus_monotonic_bounds_ns"])
                self.assertEqual(value["raw_elapsed_ns"], 0)
        self.assertEqual(clocks.compare(sample(110, 100), sample(200, 220))["start_read_skew_ns"], -10)

    def test_unavailable_clocks_are_explicit_and_do_not_prevent_other_observations(self):
        with mock.patch.object(clocks.time, "CLOCK_MONOTONIC_RAW", None, create=True), mock.patch.object(clocks.time, "monotonic_ns", side_effect=[100, 120]), mock.patch.object(clocks.time, "time_ns", side_effect=OSError("private error")):
            value = clocks.sample()
        self.assertEqual(value["raw"], dict(unavailable="unsupported"))
        self.assertEqual(value["realtime"], dict(unavailable="read_failed"))
        self.assertEqual(value["monotonic_after"], reading(120))
        finish = sample(200, 220)
        finish["monotonic_before"] = dict(unavailable="read_failed")
        compared = clocks.compare(value, finish)
        self.assertEqual(compared["status"], "monotonic_unavailable")
        self.assertIsNone(compared["finish_read_skew_ns"])
        self.assertNotIn("private error", json.dumps(value))

    def test_monotonic_reader_absence_is_reported(self):
        with mock.patch.object(clocks.time, "monotonic_ns", None):
            value = clocks.sample()
        self.assertEqual(value["monotonic_before"], dict(unavailable="unsupported"))
        self.assertEqual(value["monotonic_after"], dict(unavailable="unsupported"))

    def test_interruptions_propagate_from_every_read(self):
        for boundary in clocks.READINGS:
            with self.subTest(boundary=boundary):
                failure = InterruptedError("SIGTERM")
                before = failure if boundary == "monotonic_before" else 100
                after = failure if boundary == "monotonic_after" else 120
                raw = failure if boundary == "raw" else 1000
                realtime = failure if boundary == "realtime" else 10000
                with mock.patch.object(clocks.time, "monotonic_ns", side_effect=[before, after]), mock.patch.object(clocks.time, "CLOCK_MONOTONIC_RAW", 4, create=True), mock.patch.object(clocks.time, "clock_gettime_ns", side_effect=[raw], create=True), mock.patch.object(clocks.time, "time_ns", side_effect=[realtime]), self.assertRaisesRegex(InterruptedError, "SIGTERM"):
                    clocks.sample()

    def test_failed_reads_preserve_every_other_read(self):
        for boundary in clocks.READINGS:
            with self.subTest(boundary=boundary):
                failure = OSError("private error")
                before = failure if boundary == "monotonic_before" else 100
                after = failure if boundary == "monotonic_after" else 120
                raw = failure if boundary == "raw" else 1000
                realtime = failure if boundary == "realtime" else 10000
                with mock.patch.object(clocks.time, "monotonic_ns", side_effect=[before, after]), mock.patch.object(clocks.time, "CLOCK_MONOTONIC_RAW", 4, create=True), mock.patch.object(clocks.time, "clock_gettime_ns", side_effect=[raw], create=True), mock.patch.object(clocks.time, "time_ns", side_effect=[realtime]):
                    value = clocks.sample()
                expected = sample(100, 120, 1000, 10000)
                expected[boundary] = dict(unavailable="read_failed")
                self.assertEqual(value, expected)

    def test_import_and_projection_preserve_disagreement_without_epochs(self):
        record = observed_record()
        epochs = (123_456_789_000, 123_456_789_010, 987_654_321_000, 1_790_000_000_123_456_789)
        before, after, raw, realtime = epochs
        value = clocks.observation(sample(before, after, raw, realtime), sample(before + 100, after + 110, raw - 100, realtime - 30), 0)
        record["trials"][0]["command_clocks"] = value
        self.assertEqual(set(value), {"start", "finish", "finish_command_index"})
        report.validate_result(record)
        projected = sanitized.project_run(record, [])
        value = projected["trials"][0]["command_clocks"]
        self.assertEqual(value, dict(finish_command_index=0, status="bounded", epochs_withheld=True,
                                     availability={boundary: {clock: "available" for clock in clocks.READINGS} for boundary in ("start", "finish")},
                                     start_read_skew_seconds=10 / 1e9, finish_read_skew_seconds=20 / 1e9,
                                     monotonic_elapsed_bounds_seconds=[90 / 1e9, 120 / 1e9],
                                     raw_elapsed_seconds=-100 / 1e9, realtime_elapsed_seconds=-30 / 1e9,
                                     raw_minus_monotonic_bounds_seconds=[-220 / 1e9, -190 / 1e9],
                                     realtime_minus_monotonic_bounds_seconds=[-150 / 1e9, -120 / 1e9]))
        encoded = json.dumps(projected)
        for epoch in epochs:
            self.assertNotIn(str(epoch), encoded)
            self.assertNotIn(json.dumps(epoch / 1e9), encoded)
        self.assertNotIn("value_ns", encoded)
        self.assertEqual(projected["command_clocks"]["policy"], clocks.POLICY)

    def test_corrupt_imports_fail_before_export(self):
        mutations = [
            (lambda value: value.update(comparison=clocks.comparison(value)), "invalid command clock observation"),
            (lambda value: value["start"]["raw"].update(value_ns=True), "signed 64-bit integers"),
            (lambda value: value["start"]["raw"].update(value_ns=2**80), "signed 64-bit integers"),
            (lambda value: value["start"]["raw"].update(value_ns=-(2**63) - 1), "signed 64-bit integers"),
            (lambda value: value["start"]["raw"].update(value_ns=1.5), "signed 64-bit integers"),
            (lambda value: value["start"].update(private_path="secret"), "invalid command clock sample"),
            (lambda value: value["start"].update(raw=dict(unavailable="/home/u/secret")), "invalid command clock availability"),
            (lambda value: value["start"].update(raw=dict(unavailable=[])), "invalid command clock availability"),
            (lambda value: value.update(finish_command_index=True), "invalid command clock completion index"),
            (lambda value: value.update(finish_command_index=1), "invalid command clock completion index"),
            (lambda value: value.update(finish=None, finish_command_index=None), "omit a recorded child completion"),
        ]
        for mutate, message in mutations:
            record = observed_record()
            mutate(record["trials"][0]["command_clocks"])
            for consumer in (report.validate_result, lambda value: sanitized.project_run(value, [])):
                with self.subTest(message=message, consumer=consumer), self.assertRaisesRegex(ValueError, message):
                    consumer(record)

    def test_export_allowlists_status_and_availability_independently(self):
        value = clocks.observation(sample(), sample(200, 220), 0)
        value["start"]["raw"] = dict(unavailable="/home/u/secret")
        with self.assertRaisesRegex(ValueError, "invalid command clock availability"):
            clocks.project(value)
        value["start"]["raw"] = reading(1000)
        observed = dict(clocks.comparison(value), status="/home/u/secret")
        with mock.patch.object(clocks, "comparison", return_value=observed), self.assertRaisesRegex(ValueError, "invalid command clock comparison status"):
            clocks.project(value)

    def test_primitive_history_uses_current_diagnostics_and_retains_acquisition_policy(self):
        record = observed_record()
        plain = sample_run("b" * 32)
        derive = clocks.compare
        def additional_diagnostic(start, finish):
            return dict(derive(start, finish), raw_rate_ppm=None)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "runs"
            source.mkdir()
            (source / "observed.json").write_text(json.dumps(record))
            (source / "plain.json").write_text(json.dumps(plain))
            with mock.patch.object(clocks, "compare", side_effect=additional_diagnostic), mock.patch.object(clocks, "POLICY", "command-boundary-bracketed-v2"):
                report.render(root, root / "report")
                projected = sanitized.project_run(record, [])
            history = json.loads((root / "report" / "history.json").read_text())
            page = (root / "report" / "index.html").read_text()
        self.assertEqual(history["runs"], [record, plain])
        self.assertIn('"raw_rate_ppm":null', page)
        self.assertNotIn("raw_rate_ppm", json.dumps(projected))
        self.assertEqual(projected["command_clocks"]["policy"], "command-boundary-bracketed-v1")

    def test_policy_and_execution_evidence_are_required(self):
        for mutate in (lambda record: record["context"].pop("command_clocks"),
                       lambda record: record["context"].update(topology="loopback"),
                       lambda record: record["context"].update(command_clocks=[]),
                       lambda record: record["context"].update(command_clocks="unsupported-policy"),
                       lambda record: record["trials"][0].pop("command_clocks")):
            record = observed_record()
            mutate(record)
            with self.assertRaisesRegex(ValueError, "command.clock|command clock"):
                report.validate_result(record)

    def test_failed_import_requires_real_completed_exit_codes(self):
        for codes in ([None], [False], [True], ["0"], {"anything": None}, {}):
            record = observed_record()
            record.update(status="failed", summaries=[], trials=record["trials"][:1])
            row = record["trials"][0]
            row.update(status="failed", validation=dict(ok=False), exit_codes=codes)
            for consumer in (report.validate_result, lambda value: sanitized.project_run(value, [])):
                with self.subTest(codes=codes, consumer=consumer), self.assertRaises(ValueError):
                    consumer(record)

    def test_preexecution_failure_can_omit_observations(self):
        record = observed_record()
        record.update(status="failed", summaries=[], trials=record["trials"][:1])
        row = record["trials"][0]
        row.update(status="failed", commands=[], exit_codes=[], validation=dict(ok=False))
        row.pop("command_clocks")
        report.validate_result(record)

    def test_legacy_projection_and_identity_remain_unextended(self):
        record = sample_run()
        expected = copy.deepcopy(sanitized.project_run(record, []))
        self.assertIsNone(measurements.identity(record["context"]))
        self.assertNotIn("command_clocks", json.dumps(expected))
        self.assertEqual(sanitized.project_run(record, []), expected)
        context = copy.deepcopy(record["context"])
        context["command_clocks"] = clocks.POLICY
        self.assertEqual(measurements.identity(context), dict(command_clocks=clocks.POLICY))

    def test_summary_contains_qualification_and_signed_bounds(self):
        record = observed_record()
        with tempfile.TemporaryDirectory() as directory:
            run._persist(Path(directory), record)
            summary = (Path(directory) / "summary.md").read_text()
        self.assertIn("RAW is not an external reference", summary)
        self.assertIn("[-0.000000050, -0.000000020]", summary)

    def test_html_report_contains_qualified_observations(self):
        record = observed_record()
        record["context"]["runner_label"] = "__CLOCK_QUALIFICATION_JSON__"
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "result.json"
            source.write_text(json.dumps(record))
            output = Path(directory) / "report"
            report.render(source, output)
            page = (output / "index.html").read_text()
            history = json.loads((output / "history.json").read_text())
            state = test_report.ReportTests.dashboard_state(self, output)
            source.write_text(json.dumps(sample_run()))
            plain_output = Path(directory) / "plain-report"
            report.render(source, plain_output)
            plain_state = test_report.ReportTests.dashboard_state(self, plain_output)
        self.assertIn("Command clock observations", state["table"])
        self.assertIn("RAW is not an external reference", state["table"])
        self.assertIn("bounded [0.000000010, 0.000000040] [-0.000000050, -0.000000020] 0.000000010 / 0.000000020 All available", state["table"])
        self.assertEqual(state["points"], 1)
        self.assertNotIn("Command clock observations", plain_state["table"])
        self.assertEqual(plain_state["points"], 1)
        self.assertIn('"runner_label":"__CLOCK_QUALIFICATION_JSON__"', page)
        self.assertEqual(history["runs"][0]["trials"][0]["command_clocks"], record["trials"][0]["command_clocks"])

    def test_remote_option_rejected_before_output_or_probes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            with mock.patch.object(run, "_tool") as probe, contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                run.main(["--command-clocks", "--mode", "loopback", "--output", str(output)])
            probe.assert_not_called()
            self.assertFalse(output.exists())


class ClockExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_runner_opt_in_separates_identity_and_preserves_legacy_exports(self):
        args = test_experiment_boundaries.ExperimentBoundaries.fixture(self)
        baseline = run.main([*args, "--output", str(self.root / "legacy")])
        enabled = run.main([*args, "--command-clocks", "--output", str(self.root / "observed")])
        report.validate_result(enabled)
        self.assertEqual(enabled["context"]["command_clocks"], clocks.POLICY)
        self.assertEqual(len(enabled["trials"]), 1)
        self.assertIn("command_clocks", enabled["trials"][0])
        self.assertNotEqual(baseline["summaries"][0]["series_id"], enabled["summaries"][0]["series_id"])
        self.assertNotIn("command_clocks", json.dumps(sanitized.project_run(baseline, [])))
        self.assertIn("command_clocks", sanitized.project_run(enabled, []))

    def test_clock_preflight_failure_is_importable_before_probes(self):
        output = self.root / "bad-preflight"
        with mock.patch.object(run, "_tool") as probe, self.assertRaises(ValueError):
            run.main(["--command-clocks", "--repetitions", "0", "--output", str(output)])
        probe.assert_not_called()
        record = report.parse_result((output / "results.json").read_text())
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["trials"], [])
        self.assertIn("measurement_environment", record["context"])
        self.assertIn("command_clocks", sanitized.project_run(record, []))

    def test_raw_read_failure_still_records_other_clocks(self):
        with mock.patch.object(clocks.time, "CLOCK_MONOTONIC_RAW", 4, create=True), mock.patch.object(clocks.time, "clock_gettime_ns", side_effect=OSError("unavailable"), create=True):
            value = clocks.sample()
        self.assertEqual(value["raw"], dict(unavailable="read_failed"))
        self.assertIn("value_ns", value["realtime"])

    def test_real_sleep_and_cpu_commands_keep_existing_elapsed_policy(self):
        for index, program in enumerate(("import time; time.sleep(.03)", "sum(i*i for i in range(100000))")):
            outcome = run.execute_commands([[sys.executable, "-c", program]], self.root / str(index), 5, command_clocks=True)
            self.assertTrue(outcome["ok"])
            self.assertGreater(outcome["elapsed_seconds"], 0)
            clocks.validate_observation(outcome["command_clocks"], outcome["exit_codes"])
            self.assertEqual(outcome["command_clocks"]["finish_command_index"], 0)

    def test_disabled_option_does_not_read_clock_observations(self):
        with mock.patch.object(clocks, "sample") as observe:
            outcome = run.execute_commands([[sys.executable, "-c", "pass"]], self.root / "legacy", 5)
        observe.assert_not_called()
        self.assertTrue(outcome["ok"])
        self.assertNotIn("command_clocks", outcome)

    def test_missing_executable_retains_start_without_inventing_completion(self):
        outcome = run.execute_commands([[str(self.root / "missing")]], self.root / "missing-log", 5, command_clocks=True)
        self.assertFalse(outcome["ok"])
        value = outcome["command_clocks"]
        self.assertIsNone(value["finish"])
        self.assertIsNone(value["finish_command_index"])
        self.assertEqual(clocks.comparison(value)["status"], "no_completed_child")
        clocks.validate_observation(value, outcome["exit_codes"])

    def test_timeout_keeps_clock_observation_of_terminated_child(self):
        outcome = run.execute_commands([[sys.executable, "-c", "import time; time.sleep(5)"]], self.root / "timeout", .05, command_clocks=True)
        self.assertTrue(outcome["timed_out"])
        self.assertFalse(outcome["ok"])
        clocks.validate_observation(outcome["command_clocks"], outcome["exit_codes"])

    def test_last_exit_owns_end_even_when_earlier_waiter_samples_later(self):
        first_sample = threading.Event()
        second_sample = threading.Event()
        start = sample()
        early = sample(300, 320, 3000, 30000)
        owner = sample(200, 220, 2000, 20000)
        main_thread = threading.current_thread()
        sample_lock = threading.Lock()
        calls = 0
        def delayed_sample():
            nonlocal calls
            if threading.current_thread() is main_thread:
                return start
            with sample_lock:
                calls += 1
                number = calls
            if number == 1:
                first_sample.set()
                if not second_sample.wait(3):
                    raise AssertionError("second child did not finish")
                return early
            second_sample.set()
            return owner
        class Child:
            def __init__(self, index):
                self.index = index
                self.pid = 1000000 + index
                self.returncode = None
            def wait(self):
                if self.index == 0 and not first_sample.wait(3):
                    raise AssertionError("first waiter did not sample")
                self.returncode = 0
                return 0
        children = [Child(0), Child(1)]
        with mock.patch.object(run.subprocess, "Popen", side_effect=children), mock.patch.object(clocks, "sample", side_effect=delayed_sample), mock.patch.object(run.os, "killpg", side_effect=ProcessLookupError):
            outcome = run.execute_commands([["first"], ["second"]], self.root / "ordered", 5, command_clocks=True)
        self.assertTrue(outcome["ok"])
        self.assertEqual(outcome["command_clocks"]["finish_command_index"], 0)
        self.assertEqual(outcome["command_clocks"]["finish"], owner)

    def test_clock_read_delay_after_completion_does_not_create_timeout(self):
        main_thread = threading.current_thread()
        sampling = threading.Event()
        original_start = threading.Thread.start
        def delayed_sample():
            if threading.current_thread() is not main_thread:
                sampling.set()
                time.sleep(.1)
                return sample(200, 220, 1130, 10070)
            return sample()
        def start(thread):
            original_start(thread)
            if not sampling.wait(3):
                raise AssertionError("completion sampler did not start")
        class Child:
            pid = 1000000
            returncode = None
            def wait(self):
                self.returncode = 0
                return 0
        def monotonic():
            return .01 if threading.current_thread() is not main_thread else int(sampling.is_set())
        with mock.patch.object(run.subprocess, "Popen", return_value=Child()), mock.patch.object(run.time, "monotonic", side_effect=monotonic), mock.patch.object(clocks, "sample", side_effect=delayed_sample), mock.patch.object(run.threading.Thread, "start", start), mock.patch.object(run.os, "killpg", side_effect=ProcessLookupError):
            outcome = run.execute_commands([["immediate-child"]], self.root / "slow-clock", .08, command_clocks=True)
        self.assertTrue(outcome["ok"])
        self.assertFalse(outcome["timed_out"])
        self.assertEqual(outcome["elapsed_seconds"], .01)


if __name__ == "__main__":
    unittest.main()
