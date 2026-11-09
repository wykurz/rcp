"""Offline contracts for bounded diagnostics and optional harness instrumentation."""

import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from benchmarks import measurements, report, run, telemetry


class Clock:
    def __init__(self):
        self.now = 0
    def __call__(self):
        return self.now


class Metrics:
    def __init__(self):
        self.value = 100
    def setup(self):
        return {"cpu_model": dict(status="unavailable")}
    def snapshot(self):
        groups = {key: dict(status="available", values={"total": 1}) for key in telemetry.GROUPS}
        groups.update(cgroup_cpu=dict(status="available", values={"usage_usec": self.value}),
            cgroup_io=dict(status="available", values={"rbytes": self.value, "wbytes": self.value}),
            cgroup_memory=dict(status="available", values={"current_bytes": self.value, "peak_bytes": 1000}))
        return groups


class TelemetryTests(unittest.TestCase):
    def wait_for(self, predicate):
        deadline = time.monotonic() + 2
        while not predicate() and time.monotonic() < deadline:
            time.sleep(.001)
        self.assertTrue(predicate())

    def phase_samples(self, sampler, phase):
        return sum(item["samples"] for item in sampler.snapshot()["phases"] if item["phase"] == phase)

    def write(self, root, file, text):
        path = root / file.lstrip("/")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def reader(self, root):
        self.write(root, "/proc/self/cgroup", "0::/owner/job\n")
        self.write(root, "/proc/self/mountinfo", "35 20 0:30 /owner /sys/fs/cgroup rw - cgroup2 cgroup rw\n")
        self.write(root, "/proc/stat", "cpu 1 2 3 4 5 6 7 8 9 10\ncpu0 1 2\nprivate SECRET\n")
        self.write(root, "/proc/meminfo", "MemTotal: 100 kB\nMemAvailable: 20 kB\nSECRET: 123 kB\n")
        for kind in ("cpu", "memory", "io"):
            text = "some avg10=1.25 avg60=2.00 avg300=3.00 total=100\n"
            self.write(root, "/proc/pressure/" + kind, text)
            self.write(root, "/sys/fs/cgroup/job/" + kind + ".pressure", text)
        for file, text in (("cpu.stat", "usage_usec 100\nuser_usec 50\nSECRET 999\n"),
                ("memory.events", "oom 1\noom_kill 0\nSECRET 999\n"),
                ("memory.current", "1024\n"), ("memory.peak", "2048\n"),
                ("io.stat", "8:0 rbytes=3 wbytes=5 rios=1 wios=2 dbytes=0 dios=0\n")):
            self.write(root, "/sys/fs/cgroup/job/" + file, text)
        self.write(root, "/proc/diskstats", "8 0 PRIVATE_DEVICE 1 0 2 3 4 0 5 6 7 8 9\n")
        value = telemetry.Reader(root, root, root=root)
        value.devices = {"source": (8, 0), "destination": (8, 0)}
        return value

    def test_reads_actual_mounted_cgroup_and_only_numeric_allowlists(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reader = self.reader(root)
            self.assertEqual(reader.cgroup, Path("/sys/fs/cgroup/job"))
            sample = reader.snapshot()
            self.assertEqual(sample["host_cpu"]["values"]["steal_ticks"], 8)
            self.assertEqual(sample["host_memory"]["values"]["MemTotal_bytes"], 102400)
            self.assertEqual(sample["cgroup_io"]["values"], dict(rbytes=3, wbytes=5, rios=1, wios=2))
            self.assertEqual(sample["cgroup_memory"]["values"], dict(current_bytes=1024, peak_bytes=2048))
            self.assertEqual(sample["source_io"]["values"]["read_sectors"], 2)
            text = json.dumps(sample)
            for private in ("SECRET", "PRIVATE_DEVICE", "/owner", "/sys", str(root)):
                self.assertNotIn(private, text)

    def test_missing_or_invalid_data_is_unavailable_without_raw_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reader = self.reader(root)
            self.write(root, "/proc/meminfo", "MemTotal: 100 kB\nMemTotal: 200 kB\n")
            self.write(root, "/proc/stat", "cpu -1 2 3 4 5 6 7 8\n")
            self.write(root, "/sys/fs/cgroup/job/cpu.stat", "usage_usec 1\nusage_usec 2\n")
            self.write(root, "/sys/fs/cgroup/job/io.stat", "8:0 rbytes=1 rbytes=2 wbytes=3 rios=4 wios=5\n")
            (root / "proc/pressure/cpu").unlink()
            sample = reader.snapshot()
            for group in ("host_cpu", "host_memory", "cgroup_cpu", "cgroup_io", "host_cpu_pressure"):
                self.assertEqual(sample[group]["status"], "unavailable")
            self.assertNotIn(str(root), json.dumps(sample))
            self.assertNotIn("No such file", json.dumps(sample))
            self.write(root, "/proc/self/cgroup", "0::/../../SECRET\n")
            rejected = telemetry.Reader(root, root, root=root)
            self.assertIsNone(rejected.cgroup)
            self.assertEqual(rejected.snapshot()["cgroup_cpu"]["reason"], "unsupported")

    def test_numeric_parsers_reject_nonfinite_negative_duplicate_and_oversized_values(self):
        for value in ("-1", "nan", "1.0", "True", str(1 << 63)):
            with self.subTest(value=value), self.assertRaises(ValueError):
                telemetry.integer(value)
        for value in ("nan", "inf", "-1", "100.1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                telemetry.pressure("some avg10=" + value + " avg60=0 avg300=0 total=1")
        with self.assertRaises(ValueError):
            telemetry.pressure("some avg10=1 avg60=0 avg300=0 total=1\nsome avg10=1 avg60=0 avg300=0 total=1")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reader = self.reader(root)
            self.write(root, "/proc/stat", "x" * (telemetry.READ_BYTES + 1))
            self.assertEqual(reader.snapshot()["host_cpu"]["reason"], "invalid")

    def test_setup_uses_bounded_cpu_prefix_and_never_hostname_or_serial(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reader = self.reader(root)
            self.write(root, "/proc/cpuinfo", ("model name : Example CPU 123\nserial : SECRET\n" * 4000))
            with mock.patch.object(telemetry.platform, "system", return_value="Linux"), \
                    mock.patch.object(telemetry.platform, "release", return_value="6.8.0-test"), \
                    mock.patch.object(telemetry.platform, "machine", return_value="x86_64"), \
                    mock.patch.object(telemetry.os, "cpu_count", return_value=96):
                setup = reader.setup()
            self.assertEqual(setup["cpu_model"], dict(status="available", value="Example CPU 123"))
            self.assertEqual(setup["logical_cpu_count"]["value"], 96)
            self.assertEqual(setup["host_memory_total_bytes"]["value"], 102400)
            self.assertNotIn("SECRET", json.dumps(setup))
            self.assertEqual(set(setup), {"cpu_model", "logical_cpu_count", "host_memory_total_bytes", "os", "kernel", "architecture"})

    def test_phase_summaries_attribute_deltas_and_withhold_reset_counters(self):
        with tempfile.TemporaryDirectory() as directory:
            clock, reader = Clock(), Metrics()
            sampler = telemetry.Sampler(directory, directory, directory, reader=reader, clock=clock).start()
            self.wait_for(lambda: sampler.snapshot()["samples"] > 0)
            sampler.mark("generation", 0)
            self.wait_for(lambda: self.phase_samples(sampler, "generation") > 0)
            clock.now = 10
            reader.value = 150
            sampler.sample()
            self.wait_for(lambda: self.phase_samples(sampler, "generation") >= 2)
            sampler.mark("command", 0, 0)
            self.wait_for(lambda: self.phase_samples(sampler, "command") > 0)
            clock.now = 20
            reader.value = 200
            sampler.sample()
            self.wait_for(lambda: self.phase_samples(sampler, "command") >= 2)
            clock.now = 30
            reader.value = 1
            sampler.sample()
            self.wait_for(lambda: self.phase_samples(sampler, "command") >= 3)
            sampler.finish(False)
            result = sampler.snapshot()
            self.assertEqual(result["status"], "partial")
            generation = next(item for item in result["phases"] if item["phase"] == "generation")
            self.assertEqual(generation["elapsed_seconds"], 10)
            self.assertEqual(generation["metrics"]["cpu_usage_usec"]["delta"], 50)
            trial = sampler.snapshot(0)
            self.assertEqual(len(trial["phases"]), 1)
            self.assertEqual(trial["phases"][0]["status"], "interrupted")
            counter = trial["phases"][0]["metrics"]["cpu_usage_usec"]
            self.assertEqual(counter["status"], "counter_reset")
            self.assertIsNone(counter["delta"])
            self.assertEqual(trial["phases"][0]["metrics"]["memory_current_bytes"]["maximum"], 200)
            lines = [json.loads(line) for line in (Path(directory) / telemetry.ARTIFACT).read_text().splitlines()]
            self.assertEqual(sum(item["kind"] == "sample" for item in lines), result["samples"])
            self.assertTrue(any(item["kind"] == "phase" and item["phase"] == "command" for item in lines))
            self.assertEqual(lines[-1]["trial_index"], 0)
            self.assertNotIn(directory, json.dumps(lines))

    def test_sample_byte_time_and_phase_bounds_stop_without_failing_payload(self):
        for bound, value, reason in (("MAX_SAMPLES", 2, "sample_limit"), ("MAX_BYTES", 1000, "byte_limit"),
                ("MAX_SECONDS", 1, "time_limit"), ("MAX_PHASES", 2, "phase_limit")):
            with self.subTest(bound=bound), tempfile.TemporaryDirectory() as directory:
                clock = Clock()
                with mock.patch.object(telemetry, bound, value):
                    sampler = telemetry.Sampler(directory, directory, directory, reader=Metrics(), clock=clock).start()
                    self.wait_for(lambda: sampler.snapshot()["samples"] > 0 or sampler.snapshot()["reason"] is not None)
                    for index in range(10):
                        clock.now = index
                        previous = sampler.snapshot()["samples"]
                        sampler.mark("command", 0, index)
                        self.wait_for(lambda: sampler.snapshot()["samples"] > previous or sampler.snapshot()["reason"] is not None)
                    sampler.finish(True)
                    snapshot = sampler.snapshot()
                    self.assertEqual(snapshot["reason"], reason)
                    self.assertLessEqual(snapshot["samples"], telemetry.MAX_SAMPLES)
                    self.assertLessEqual(snapshot["bytes"], telemetry.MAX_BYTES)
                    self.assertLessEqual(len(snapshot["phases"]), telemetry.MAX_PHASES)
                    self.assertLessEqual((Path(directory) / telemetry.ARTIFACT).stat().st_size, telemetry.MAX_BYTES)

    def test_unwritable_artifact_and_sampler_read_failures_do_not_raise(self):
        with tempfile.TemporaryDirectory() as directory:
            reader = Metrics()
            with mock.patch.object(reader, "snapshot", side_effect=OSError("SECRET /private/path")):
                sampler = telemetry.Sampler(directory, directory, directory, reader=reader).start()
                self.wait_for(lambda: sampler.snapshot()["samples"] > 0)
                sampler.finish(True)
            self.assertEqual(sampler.snapshot()["status"], "partial")
            self.assertNotIn("SECRET", (Path(directory) / telemetry.ARTIFACT).read_text())
            unavailable = telemetry.Sampler(Path(directory) / "missing", directory, directory, reader=reader).start()
            self.wait_for(lambda: unavailable.snapshot()["reason"] is not None)
            unavailable.finish(True)
            self.assertEqual(unavailable.snapshot()["status"], "unavailable")
            self.assertEqual(unavailable.snapshot()["reason"], "artifact_unavailable")

    def test_optional_thread_start_failure_is_unavailable_not_a_payload_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            sampler = telemetry.Sampler(directory, directory, directory, reader=Metrics())
            with mock.patch.object(threading.Thread, "start", side_effect=RuntimeError("SECRET thread limit")):
                self.assertIs(sampler.start(), sampler)
            sampler.mark("command", 0, 0)
            sampler.finish(True)
            self.assertEqual(sampler.snapshot()["status"], "unavailable")
            self.assertEqual(sampler.snapshot()["reason"], "artifact_unavailable")
            self.assertNotIn("SECRET", json.dumps(sampler.snapshot()))
            sampler.start()
            self.assertIsNone(sampler.thread)

    @unittest.skipUnless(os.name == "posix", "POSIX signal contract")
    def test_sigterm_during_thread_start_closes_the_owned_sampler(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.harness(root)
            original_start = threading.Thread.start
            started = []
            def start(thread):
                original_start(thread)
                started.append(thread)
                signal.raise_signal(signal.SIGTERM)
            def interrupted(*args):
                raise InterruptedError("SIGTERM")
            previous = signal.signal(signal.SIGTERM, interrupted)
            try:
                with mock.patch.object(threading.Thread, "start", start), \
                        mock.patch.object(telemetry, "Reader", return_value=Metrics()), \
                        self.assertRaises(InterruptedError):
                    run.main([*args, "--system-telemetry", "--output", str(root / "out")])
            finally:
                signal.signal(signal.SIGTERM, previous)
            self.assertEqual(len(started), 1)
            self.assertFalse(started[0].is_alive())
            result = json.loads((root / "out/results.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["system_telemetry"]["phases"][-1]["status"], "interrupted")

    def test_periodic_sampler_stops_and_close_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            sampler = telemetry.Sampler(directory, directory, directory, reader=Metrics()).start()
            self.wait_for(lambda: sampler.snapshot()["samples"] > 0)
            sampler.finish(True)
            self.assertFalse(sampler.thread.is_alive())
            previous = sampler.snapshot()
            sampler.finish(False)
            sampler.sample()
            self.assertEqual(sampler.snapshot(), previous)

    def test_stalled_initial_discovery_and_setup_never_block_benchmark_lifecycle(self):
        for stage in ("discovery", "setup"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                entered, release = threading.Event(), threading.Event()
                readers = []
                class StalledReader(Metrics):
                    def __init__(self, *args):
                        readers.append(threading.current_thread())
                        super().__init__()
                        if stage == "discovery":
                            entered.set()
                            release.wait(5)
                    def setup(self):
                        readers.append(threading.current_thread())
                        if stage == "setup":
                            entered.set()
                            release.wait(5)
                        return super().setup()
                with mock.patch.object(telemetry, "Reader", StalledReader):
                    started = time.monotonic()
                    sampler = telemetry.Sampler(directory, directory, directory).start()
                    self.assertLess(time.monotonic() - started, .5)
                    self.assertTrue(entered.wait(2))
                    try:
                        started = time.monotonic()
                        sampler.mark("command", 0, 0)
                        self.assertLess(time.monotonic() - started, .5)
                        sampler.finish(False)
                        self.assertLess(time.monotonic() - started, 1.5)
                        result = sampler.snapshot()
                        self.assertEqual(result["reason"], "reader_stalled")
                        self.assertEqual(result["samples"], 0)
                        self.assertEqual(result["phases"][-1]["phase"], "command")
                        self.assertTrue(all(value["status"] == "unavailable" for value in sampler.system_setup.values()))
                        self.assertTrue(all(thread is not threading.main_thread() for thread in readers))
                    finally:
                        release.set()
                        sampler.thread.join(timeout=2)
                    self.assertFalse(sampler.thread.is_alive())

    def test_stalled_phase_read_preserves_cached_sample_without_fresh_attribution(self):
        with tempfile.TemporaryDirectory() as directory:
            entered, release = threading.Event(), threading.Event()
            reader = Metrics()
            original = reader.snapshot
            calls = []
            def read():
                calls.append(threading.current_thread())
                if len(calls) > 1:
                    entered.set()
                    release.wait(5)
                return original()
            reader.snapshot = read
            sampler = telemetry.Sampler(directory, directory, directory, reader=reader).start()
            self.wait_for(lambda: sampler.snapshot()["samples"] > 0)
            sampler.mark("command", 0, 0)
            self.assertTrue(entered.wait(2))
            try:
                started = time.monotonic()
                sampler.mark("source", 0, 0)
                sampler.finish(False)
                self.assertLess(time.monotonic() - started, 1.5)
                result = sampler.snapshot()
                self.assertEqual(result["reason"], "reader_stalled")
                self.assertGreater(result["samples"], 0)
                self.assertEqual(self.phase_samples(sampler, "source"), 0)
                self.assertEqual(result["phases"][-1]["metrics"]["cpu_usage_usec"], dict(status="unavailable"))
                self.assertTrue(all(thread is not threading.main_thread() for thread in calls))
            finally:
                release.set()
                sampler.thread.join(timeout=2)
            self.assertFalse(sampler.thread.is_alive())
            self.assertEqual(self.phase_samples(sampler, "source"), 0)

    def test_finish_never_starts_a_new_final_read(self):
        with tempfile.TemporaryDirectory() as directory:
            reader = Metrics()
            calls = []
            def read():
                calls.append(threading.current_thread())
                if len(calls) > 1:
                    raise AssertionError("unexpected final read")
                return Metrics.snapshot(reader)
            reader.snapshot = read
            sampler = telemetry.Sampler(directory, directory, directory, reader=reader).start()
            self.wait_for(lambda: sampler.snapshot()["samples"] == 1)
            sampler.finish(True)
            self.assertEqual(len(calls), 1)
            self.assertIsNot(calls[0], threading.main_thread())
            self.assertEqual(sampler.snapshot()["samples"], 1)

    def test_host_device_and_pressure_summaries_preserve_units_and_gauge_minimum(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reader = self.reader(root)
            sampler = telemetry.Sampler(root, root, root, reader=reader).start()
            self.wait_for(lambda: sampler.snapshot()["samples"] > 0)
            sampler.mark("command", 0, 0)
            self.wait_for(lambda: self.phase_samples(sampler, "command") > 0)
            self.write(root, "/proc/meminfo", "MemTotal: 100 kB\nMemAvailable: 10 kB\n")
            self.write(root, "/proc/stat", "cpu 11 2 3 14 15 6 7 8\n")
            self.write(root, "/proc/diskstats", "8 0 PRIVATE 1 0 12 3 4 0 15 6 7 18 9\n")
            self.write(root, "/proc/pressure/io", "some avg10=1 avg60=2 avg300=3 total=150\n")
            sampler.sample()
            self.wait_for(lambda: self.phase_samples(sampler, "command") >= 2)
            sampler.finish(True)
            metrics = sampler.snapshot(0)["phases"][0]["metrics"]
            self.assertEqual(metrics["host_cpu_total_ticks"]["delta"], 30)
            self.assertEqual(metrics["host_cpu_idle_ticks"]["delta"], 10)
            self.assertEqual(metrics["host_memory_available_bytes"]["minimum"], 10240)
            self.assertEqual(metrics["source_read_sectors"]["delta"], 10)
            self.assertEqual(metrics["destination_io_milliseconds"]["delta"], 10)
            self.assertEqual(metrics["host_io_pressure_some_usec"]["delta"], 50)
            self.assertEqual(metrics["host_io_pressure_full_usec"], dict(status="unavailable"))

    def test_attach_owns_each_phase_once_and_keeps_global_artifact_counts_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            sampler = telemetry.Sampler(directory, directory, directory, reader=Metrics()).start()
            self.wait_for(lambda: sampler.snapshot()["samples"] > 0)
            for phase, case, trial in (("generation", 0, None), ("command", 0, 0), ("cleanup", 0, 0),
                    ("generation", 1, None), ("command", 1, 1)):
                previous = sampler.snapshot()["samples"]
                sampler.mark(phase, case, trial)
                self.wait_for(lambda: sampler.snapshot()["samples"] > previous)
            sampler.finish(True)
            complete = sampler.snapshot()
            record = dict(trials=[{}, {}])
            sampler.attach(record)
            root = record["system_telemetry"]
            summaries = [root, *(trial["system_telemetry"] for trial in record["trials"])]
            self.assertEqual(sum(len(item["phases"]) for item in summaries), len(complete["phases"]))
            self.assertTrue(all(phase["trial_index"] is None for phase in root["phases"]))
            for index, trial in enumerate(record["trials"]):
                self.assertTrue(all(phase["trial_index"] == index for phase in trial["system_telemetry"]["phases"]))
            self.assertEqual(sum(item["samples"] for item in summaries), complete["samples"])
            self.assertTrue(all(item["artifact_samples"] == complete["samples"] for item in summaries))
            self.assertTrue(all(item["bytes"] == complete["bytes"] for item in summaries))

    def test_declared_phase_cap_with_rich_64bit_counters_fits_existing_raw_json_parser(self):
        from infra.aws_benchmark import results
        from benchmarks.test_measurements import TIME
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.harness(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["cases"] = [dict(manifest["cases"][0], id="tiny-" + str(index)) for index in range(4)]
            manifest_path.write_text(json.dumps(manifest))
            case_option = args.index("--case")
            del args[case_option:case_option + 2]
            for case in manifest["cases"]:
                args += ["--case", case["id"]]
            record = run.main([*args, "--baseline-bin-dir", str(root / "bin"), "--paired-seed", "0",
                "--repetitions", "2", "--cache", "source-warm", "--streaming-verification", "--verification-mode", "metadata-size",
                "--local-resources", "--system-telemetry", "--resource-time", TIME, "--output", str(root / "run")])
            self.assertEqual(len(record["trials"]), 16)
            class Rich(Metrics):
                def snapshot(self):
                    groups = {key: dict(status="available", values={}) for key in telemetry.GROUPS}
                    for group, field, _, _ in telemetry.SUMMARY_METRICS:
                        groups[group]["values"][field] = telemetry.MAX_NUMBER
                    return groups
            output = root / "diagnostics"
            output.mkdir()
            sampler = telemetry.Sampler(output, root, root, reader=Rich()).start()
            self.wait_for(lambda: sampler.snapshot()["samples"] > 0)
            for index in range(telemetry.MAX_PHASES - 1):
                previous = sampler.snapshot()["samples"]
                sampler.mark("command", index % 4, index % 16)
                self.wait_for(lambda: sampler.snapshot()["samples"] > previous)
            sampler.finish(True)
            sampler.attach(record)
            total = len(record["system_telemetry"]["phases"]) + sum(
                len(trial["system_telemetry"]["phases"]) for trial in record["trials"])
            self.assertEqual(total, telemetry.MAX_PHASES)
            raw = json.dumps(record, separators=(",", ":")).encode()
            self.assertEqual(results.json_value(raw, results.MAX_JSON), record)
            count, pending = 0, [record]
            while pending:
                item = pending.pop()
                count += 1
                if isinstance(item, dict):
                    pending.extend(item.keys())
                    pending.extend(item.values())
                elif isinstance(item, list):
                    pending.extend(item)
            self.assertLess(count, 100000)
            self.assertLess(len(raw), results.MAX_JSON)

    def test_only_fixed_descriptor_changes_series_identity(self):
        self.assertIsNone(measurements.identity({}))
        old = {"pairing": {"policy": "paired-v1"}, "command_clocks": "clock-v1"}
        self.assertEqual(measurements.identity(old), {"pairing_policy": "paired-v1", "command_clocks": "clock-v1"})
        context = dict(old, system_telemetry=telemetry.descriptor())
        self.assertEqual(measurements.identity(context)["system_telemetry"], telemetry.descriptor())
        changed = copy.deepcopy(context)
        changed["system_telemetry"]["period_seconds"] = 1
        with self.assertRaisesRegex(ValueError, "telemetry"):
            measurements.identity(changed)

    def harness(self, root, *, broken=False, block=False):
        binary = root / "bin"
        binary.mkdir()
        generator = "import pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\n"
        generator += "sys.exit(7)\n" if broken else "p=pathlib.Path(sys.argv[1])/'filegen'/'0'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')\n"
        copy_script = "import shutil,sys,time\nif '--version' in sys.argv or '--help' in sys.argv: print('rcp 1'); sys.exit(0)\n"
        copy_script += ("pathlib.Path(" + repr(str(root / "ready")) + ").touch(); time.sleep(60)\n") if block else "time.sleep(.05); shutil.copytree(sys.argv[-2],sys.argv[-1])\n"
        copy_script = "import pathlib\n" + copy_script
        for name, script in dict(filegen=generator, rcp=copy_script, rcpd="print('rcpd 1')").items():
            path = binary / name
            path.write_text("#!" + sys.executable + "\n" + script)
            path.chmod(0o755)
        manifest = root / "manifest.json"
        manifest.write_text(json.dumps(dict(schema_version=1,
            cases=[dict(id="tiny", directory_widths=[1], files_per_leaf=1, file_size_bytes=1)],
            variants=[dict(id="rcp-default", tool="rcp", args=[], processes=1)])))
        return ["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default", "--bin-dir", str(binary),
            "--source-root", str(root), "--destination-root", str(root), "--cache", "uncontrolled", "--no-timings", "--repetitions", "1"]

    def test_real_small_harness_retains_phases_and_sampling_off_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.harness(root)
            ordinary = run.main([*args, "--output", str(root / "ordinary")])
            instrumented = run.main([*args, "--system-telemetry", "--output", str(root / "sampled")])
            report.validate_result(instrumented)
            self.assertNotIn("system_telemetry", ordinary)
            self.assertNotIn("system_setup", ordinary)
            self.assertNotIn("system_telemetry", ordinary["context"])
            self.assertFalse((root / "ordinary" / telemetry.ARTIFACT).exists())
            self.assertEqual(instrumented["status"], "complete")
            self.assertEqual(instrumented["context"]["system_telemetry"], telemetry.descriptor())
            phases = {item["phase"] for summary in [instrumented["system_telemetry"],
                *(trial["system_telemetry"] for trial in instrumented["trials"])] for item in summary["phases"]}
            self.assertTrue({"generation", "initial_verification", "command", "destination", "cleanup"} <= phases)
            self.assertGreater(instrumented["trials"][0]["system_telemetry"]["samples"], 0)
            self.assertNotEqual(ordinary["summaries"][0]["series_id"], instrumented["summaries"][0]["series_id"])

    def test_generation_failure_retains_partial_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.harness(root, broken=True)
            with self.assertRaisesRegex(RuntimeError, "filegen failed"):
                run.main([*args, "--system-telemetry", "--output", str(root / "out")])
            result = json.loads((root / "out/results.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["system_telemetry"]["phases"][-1]["phase"], "generation")
            self.assertEqual(result["system_telemetry"]["phases"][-1]["status"], "interrupted")
            self.assertGreater(result["system_telemetry"]["samples"], 0)

    @unittest.skipUnless(os.name == "posix", "POSIX signal contract")
    def test_sigterm_completes_while_initial_diagnostic_reader_is_stalled(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.harness(root, block=True)
            script = """import signal,sys,threading
from unittest import mock
from benchmarks import run,telemetry
class StalledReader:
    def __init__(self,*args):
        threading.Event().wait(60)
def stop(*args):
    raise InterruptedError('SIGTERM')
signal.signal(signal.SIGTERM,stop)
with mock.patch.object(telemetry,'Reader',StalledReader):
    run.main(sys.argv[1:])
"""
            process = subprocess.Popen([sys.executable, "-B", "-c", script, *args,
                "--system-telemetry", "--output", str(root / "out")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                self.wait_for(lambda: (root / "ready").exists())
                started = time.monotonic()
                process.send_signal(signal.SIGTERM)
                self.assertNotEqual(process.wait(timeout=5), 0)
                self.assertLess(time.monotonic() - started, 3)
                result = json.loads((root / "out/results.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["system_telemetry"]["reason"], "reader_stalled")
                self.assertEqual(result["system_telemetry"]["samples"], 0)
                self.assertEqual(result["trials"][0]["system_telemetry"]["phases"][-1]["status"], "interrupted")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()

    @unittest.skipUnless(os.name == "posix", "POSIX signal contract")
    def test_sigterm_persists_partial_telemetry_and_stops_sampler(self):
        import time
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.harness(root, block=True)
            process = subprocess.Popen([sys.executable, "-B", "-m", "benchmarks.run", *args,
                "--system-telemetry", "--output", str(root / "out")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 10
                while not (root / "ready").exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue((root / "ready").exists())
                process.send_signal(signal.SIGTERM)
                self.assertNotEqual(process.wait(timeout=10), 0)
                result = json.loads((root / "out/results.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["trials"][0]["system_telemetry"]["phases"][-1]["status"], "interrupted")
                self.assertGreater(result["trials"][0]["system_telemetry"]["samples"], 0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    unittest.main()
