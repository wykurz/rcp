"""Execution failure and imported-record boundaries for optional local resources."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from benchmarks import measurements, report, run, sanitized
from benchmarks.test_report import sample_run
from benchmarks.test_measurements import GNU_TIME, TIME, build


def metrics(**changes):
    return {**{key: 0 for key in (*measurements.FLOATS, *measurements.COUNTS, "exit_code")}, **changes}


def resource_record(**changes):
    row = dict(status="failed", commands=[["copy", "source", "destination"]], exit_codes=[0], timed_out=False, launch_error=None,
               exit_status_scope=measurements.EXIT_STATUS_SCOPE,
               resources=dict(status="complete", metrics=metrics(), raw_sha256="a" * 64))
    row.update(changes)
    row.setdefault("payload_status", measurements.payload_status(row["exit_codes"], row["timed_out"], row["launch_error"]))
    return dict(context=dict(topology="local", measurement_environment={}, local_resources=dict(policy=measurements.POLICY,
        scope=measurements.SCOPE, time_sha256="b" * 64, time_version="GNU time")),
        variants=[dict(processes=1)], trials=[row])


class ResourceValidationBoundaries(unittest.TestCase):
    def test_payload_status_rejects_boolean_exits_and_unconfirmed_outcomes(self):
        self.assertEqual(measurements.payload_status([0]), "succeeded")
        for codes in ([False], [True], [], [0, 0], [0.0], [None], None, {}, (0,)):
            with self.subTest(codes=codes):
                self.assertEqual(measurements.payload_status(codes), "unconfirmed")
        for timeout in (True, 0, 1, None, "", "false"):
            with self.subTest(timeout=timeout):
                self.assertEqual(measurements.payload_status([0], timeout), "unconfirmed")
        for error in ("", "missing command", False):
            with self.subTest(error=error):
                self.assertEqual(measurements.payload_status([0], False, error), "unconfirmed")
        self.assertEqual(measurements.payload_status([0, 0], expected_commands=2), "succeeded")
        self.assertEqual(measurements.payload_status([0, False], expected_commands=2), "unconfirmed")
        self.assertEqual(measurements.payload_status([0], expected_commands=True), "unconfirmed")
        self.assertEqual(resource_record(exit_codes=[False])["trials"][0]["payload_status"], "unconfirmed")

    def test_huge_numbers_raise_value_error_and_collect_as_invalid(self):
        for field in (*measurements.FLOATS, *measurements.COUNTS):
            with self.subTest(field=field):
                value = metrics(**{field: 10 ** 400})
                with self.assertRaises(ValueError):
                    measurements.validate_metrics(value)
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "resources.json"
                    path.write_text(json.dumps(value))
                    result = measurements.collect(path, successful=True)
                    self.assertEqual(result["status"], "invalid")
                    self.assertIsNone(result["metrics"])
        for changes in (dict(phase_seconds=dict(total=10 ** 400)),
                        dict(trials=[dict(phase_seconds=dict(preparation=10 ** 400))])):
            record = dict(context={}, trials=[])
            record.update(changes)
            with self.assertRaises(ValueError):
                measurements.validate(record)

    def test_malformed_selection_builds_and_environment_raise_value_error(self):
        for variants in (None, {}, [None], [{}], [dict(processes=True)], [dict(processes=[])], [dict(processes=2)]):
            with self.subTest(variants=variants), self.assertRaises(ValueError):
                measurements.validate_selection("local", variants)
        declaration = {"rcp": build("a" * 64)}
        for tools in (None, [], {}, {"rcp": None}, {"rcp": []}, {"rcp": {}}, {"rcp": {"sha256": []}}):
            with self.subTest(tools=tools), self.assertRaises(ValueError):
                measurements.validate_builds(declaration, tools)
        for value in (None, [], {"MALLOC_CONF": 1}, {"unknown": "value"}):
            with self.subTest(environment=value), self.assertRaises(ValueError):
                measurements.validate(dict(context=dict(measurement_environment=value), trials=[]))
        # surrogateescaped Linux environment bytes are preserved by JSON serialization
        measurements.validate(dict(context=dict(measurement_environment={"MALLOC_CONF": "\udcff"}), trials=[]))
        for value in (None, {}, [None]):
            with self.subTest(trials=value), self.assertRaises(ValueError):
                measurements.validate(dict(context={}, trials=value))

    def test_failed_command_cannot_supply_accepted_metrics(self):
        for changes in (dict(exit_codes=[139]), dict(exit_codes=[False]), dict(exit_codes=[]),
                        dict(timed_out=True), dict(timed_out=0), dict(launch_error="missing executable")):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                measurements.validate(resource_record(**changes))
        with self.assertRaises(ValueError):
            measurements.validate(resource_record(resources=dict(status="complete", metrics=metrics(exit_code=7), raw_sha256="a" * 64)))
        # successful execution can still have a later destination-validation failure
        measurements.validate(resource_record())
        qualified = resource_record(exit_status_scope=measurements.EXIT_STATUS_SCOPE, payload_status="succeeded")
        measurements.validate(qualified)
        qualified["trials"][0]["payload_status"] = "unconfirmed"
        with self.assertRaises(ValueError):
            measurements.validate(qualified)

    def test_import_and_sanitization_reject_failed_execution_metrics(self):
        for code in (127, 139):
            record = sample_run(status="failed")
            record["context"].update(resource_record()["context"])
            record.update(trials=record["trials"][:1], summaries=[])
            record["trials"][0].update(resource_record(exit_codes=[code])["trials"][0])
            record["trials"][0]["validation"] = dict(ok=False)
            with self.subTest(code=code), self.assertRaisesRegex(ValueError, "successful command"):
                report.validate_result(record)
            with self.assertRaisesRegex(ValueError, "successful command"):
                sanitized.project_run(record, [])

    def test_failed_execution_never_parses_raw_usage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resources.json"
            for raw in (None, "", "not JSON", json.dumps(metrics()), json.dumps(metrics(exit_code=127))):
                if raw is not None:
                    path.write_text(raw)
                result = measurements.collect(path, successful=False)
                self.assertEqual(result["status"], "unavailable")
                self.assertEqual(result["reason"], "execution_failed")
                self.assertIsNone(result["metrics"])


@unittest.skipUnless(GNU_TIME, "requires Linux GNU time")
class ResourceExecutionBoundaries(unittest.TestCase):
    def test_signals_and_real_high_exits_are_qualified_without_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = [(f"signal-{number}", "import os,resource; resource.setrlimit(resource.RLIMIT_CORE,(0,0)); "
                        f"os.kill(os.getpid(),{number})", 128 + number) for number in (6, 9, 11)]
            scripts += [(f"exit-{number}", f"raise SystemExit({number})", number) for number in (7, 126, 127, 134, 139)]
            for name, script, expected in scripts:
                with self.subTest(name=name):
                    result = run.execute_commands([[sys.executable, "-c", script]], root / name, 5, resource_time=TIME)
                    self.assertFalse(result["ok"])
                    self.assertEqual(result["exit_codes"], [expected])
                    self.assertEqual(result["exit_status_scope"], "resource-supervisor")
                    self.assertEqual(result["payload_status"], "unconfirmed")
                    self.assertEqual(result["resources"]["status"], "unavailable")
                    self.assertIsNone(result["resources"]["metrics"])
                    self.assertEqual(result["failure"]["kind"], "command")
                    self.assertIn("unconfirmed", result["failure"]["message"])

    def test_missing_and_nonexecutable_payloads_are_not_measured(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            denied = root / "denied"
            denied.write_text("#!/bin/sh\nexit 0\n")
            denied.chmod(0o644)
            for payload, expected in ((root / "missing", 127), (denied, 126)):
                with self.subTest(payload=payload.name):
                    result = run.execute_commands([[str(payload)]], root / (payload.name + "-logs"), 5, resource_time=TIME)
                    self.assertIsNone(result["launch_error"])
                    self.assertEqual(result["exit_codes"], [expected])
                    self.assertEqual(result["payload_status"], "unconfirmed")
                    self.assertIsNone(result["resources"]["metrics"])
                    self.assertEqual(result["failure"]["kind"], "command")
            result = run.execute_commands([[sys.executable, "-c", "pass"]], root / "supervisor-logs", 5, resource_time=str(root / "missing-time"))
            self.assertEqual(result["exit_codes"], [])
            self.assertIn("missing-time", result["launch_error"])
            self.assertEqual(result["failure"]["kind"], "launch")
            self.assertIn("missing-time", result["failure"]["message"])
            self.assertNotIn("resources.json", result["failure"]["message"])

    def cleanup_child(self, ready, complete):
        return ("import signal,time\nfrom pathlib import Path\n"
                "def cleanup(number,frame):\n    time.sleep(.15)\n"
                f"    Path({str(complete)!r}).touch()\n    raise SystemExit(0)\n"
                "signal.signal(signal.SIGTERM,cleanup)\n"
                f"Path({str(ready)!r}).touch()\ntime.sleep(20)\n")

    def test_timeout_preserves_payload_sigterm_grace(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for measured in (False, True):
                with self.subTest(measured=measured):
                    ready, complete = root / f"ready-{measured}", root / f"complete-{measured}"
                    result = run.execute_commands([[sys.executable, "-c", self.cleanup_child(ready, complete)]],
                        root / f"logs-{measured}", 1, resource_time=TIME if measured else None)
                    self.assertTrue(ready.exists())
                    self.assertTrue(complete.exists())
                    self.assertTrue(result["timed_out"])
                    self.assertFalse(result["ok"])
                    if measured:
                        self.assertEqual(result["failure"]["kind"], "timeout")
                        self.assertIn("timed out", result["failure"]["message"])
                        self.assertIsNone(result["resources"]["metrics"])

    def test_sigint_preserves_payload_sigterm_grace_after_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for measured in (False, True):
                with self.subTest(measured=measured):
                    ready, complete = root / f"ready-{measured}", root / f"complete-{measured}"
                    command = [[sys.executable, "-c", self.cleanup_child(ready, complete)]]
                    script = ("from benchmarks.run import execute_commands\n"
                              f"execute_commands({command!r},{str(root / f'logs-{measured}')!r},20,resource_time={TIME if measured else None!r})")
                    process = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
                    try:
                        deadline = time.monotonic() + 5
                        while not ready.exists() and time.monotonic() < deadline:
                            time.sleep(.01)
                        self.assertTrue(ready.exists())
                        process.send_signal(signal.SIGINT)
                        self.assertNotEqual(process.wait(timeout=5), 0)
                        self.assertTrue(complete.exists())
                    finally:
                        if process.poll() is None:
                            process.kill()
                            process.wait()

    def test_resource_failure_is_primary_only_after_command_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            supervisor = root / "invalid-time"
            supervisor.write_text("#!" + sys.executable + "\nimport pathlib,sys\n"
                "pathlib.Path(sys.argv[sys.argv.index('--output')+1]).write_text('invalid')\n")
            supervisor.chmod(0o755)
            result = run.execute_commands([["unused"]], root / "logs", 5, resource_time=str(supervisor))
            self.assertEqual(result["exit_codes"], [0])
            self.assertEqual(result["resources"]["status"], "invalid")
            self.assertEqual(result["failure"]["kind"], "resources")
            self.assertEqual(result["payload_status"], "succeeded")

    def test_runner_persists_primary_failure_and_qualified_payload_status(self):
        for scenario, wrapped in (("timeout", True), ("missing-supervisor", True), ("missing-command", True),
                                  ("invalid-resources", True), ("timeout", False), ("missing-command", False), ("command-failure", False)):
            with self.subTest(scenario=scenario, wrapped=wrapped), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binary = root / "bin"
                binary.mkdir()
                supervisor = root / "time"
                supervisor.symlink_to(TIME)
                remove = ""
                if scenario in ("missing-supervisor", "missing-command"):
                    target = supervisor if scenario == "missing-supervisor" else binary / "rcp"
                    remove = f"pathlib.Path({str(target)!r}).unlink()"
                if scenario == "invalid-resources":
                    supervisor.unlink()
                    supervisor.write_text("#!" + sys.executable + "\nimport pathlib,sys\n"
                        "if '--version' in sys.argv: print('GNU time fixture'); sys.exit(0)\n"
                        "pathlib.Path(sys.argv[sys.argv.index('--output')+1]).write_text('invalid')\n")
                    supervisor.chmod(0o755)
                scripts = dict(filegen="import pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\n"
                    "p=pathlib.Path(sys.argv[1])/'filegen'/'0'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')\n" + remove,
                    rcp="import sys,time\nif '--version' in sys.argv: print('rcp 1'); sys.exit(0)\n"
                    "if '--help' in sys.argv: print('stub'); sys.exit(0)\ntime.sleep(20)\n", rcpd="print('rcpd 1')")
                if scenario == "command-failure":
                    scripts["rcp"] = scripts["rcp"].replace("time.sleep(20)", "sys.exit(7)")
                for name, script in scripts.items():
                    executable = binary / name
                    executable.write_text("#!" + sys.executable + "\n" + script + "\n")
                    executable.chmod(0o755)
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps(dict(schema_version=1,
                    cases=[dict(id="tiny", directory_widths=[1], files_per_leaf=1, file_size_bytes=1)],
                    variants=[dict(id="rcp-default", tool="rcp", args=[], processes=1)])))
                arguments = ["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default", "--bin-dir", str(binary),
                    "--source-root", str(root), "--destination-root", str(root), "--cache", "uncontrolled", "--no-timings",
                    "--repetitions", "1", "--timeout", ".5", "--output", str(root / "out"), "--purpose", "smoke"]
                if wrapped:
                    arguments.extend(["--local-resources", "--resource-time", str(supervisor)])
                with self.assertRaises(RuntimeError):
                    run.main(arguments)
                record = report.parse_result((root / "out/results.json").read_text())
                row = record["trials"][0]
                expected = {"timeout": "timed out", "missing-supervisor": "supervisor launch failed", "missing-command": "payload failure cause is unconfirmed", "invalid-resources": "resource collection failed"}[scenario] if wrapped else "command failed or timed out"
                self.assertIn(expected, row["validation"]["error"])
                self.assertIn(expected, record["error"])
                self.assertNotIn("resources.json", row["validation"]["error"])
                projected = sanitized.project_run(record, [])["trials"][0]
                stage, category = {"timeout": ("command", "timeout"), "missing-supervisor": ("outer_launch", "execution"),
                    "missing-command": ("command", "execution") if wrapped else ("outer_launch", "execution"),
                    "command-failure": ("command", "execution"), "invalid-resources": ("postflight", "validation")}[scenario]
                self.assertEqual(projected["failure"]["stage"], stage)
                self.assertEqual(projected["failure"]["category"], category)
                if wrapped:
                    self.assertIsNone(row["resources"]["metrics"])
                    self.assertEqual(row["payload_status"], "succeeded" if scenario == "invalid-resources" else "unconfirmed")
                    self.assertEqual(projected["exit_status_scope"], "resource-supervisor")
                    self.assertEqual(projected["payload_status"], row["payload_status"])
                    self.assertIsNone(projected["resources"]["metrics"])
                else:
                    self.assertEqual(row["validation"]["error"], expected)
                    self.assertNotIn("resources", row)
                    self.assertNotIn("payload_status", row)


if __name__ == "__main__":
    unittest.main()
