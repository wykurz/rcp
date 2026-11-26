"""Progress on the command clock and its matrix integration."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from benchmarks import test_matrix_tools


def message(stamp, copied=0, linked=0):
    return json.dumps(dict(schema="rcp-progress-v1", monotonic_ns=stamp,
                           files_copied=copied, hard_links_created=linked)).encode() + b"\n"


class ProgressSamples(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "0.stderr.log"

    def read(self, data, metric="files_copied"):
        from benchmarks import work_progress
        self.path.write_bytes(data)
        return work_progress.read(self.path, metric, started=6.0, elapsed=25.0, origin=1.0, files=10)

    def test_observations_align_with_the_sampler_and_select_the_operation(self):
        data = b"an unrelated diagnostic\n" + message(6_125_000_000, 1, 2) + message(16_900_000_000, 4, 7)
        self.assertEqual(self.read(data, "hard_links_created"),
                         dict(status="observed", metric="hard_links_created", command_start_ms=5000,
                              samples=[[125, 2], [10900, 7]]))

    def test_missing_partial_duplicate_reset_or_impossible_observations_do_not_invent_progress(self):
        self.assertEqual(self.read(b"warning\n"), {"status": "missing"})
        good = message(7_000_000_000, 2)
        for sample in (good.rstrip(), good.replace(b'"files_copied": 2', b'"files_copied": 1, "files_copied": 2'),
                       message(5_999_000_000, 1), message(31_100_000_000, 1),
                       good + message(17_000_000_000, 1), good + message(6_999_999_999, 3),
                       message(7_000_000_000, True), message(7_000_000_000, 11)):
            with self.subTest(sample=sample):
                self.assertEqual(self.read(sample), {"status": "invalid"})

    def test_a_large_log_or_replaced_log_is_bounded(self):
        from benchmarks import work_progress
        self.assertEqual(self.read(b"x" * (work_progress.MAX_BYTES + 1)), {"status": "limit"})
        link = self.path.with_name("other.log")
        link.symlink_to(self.path)
        self.assertEqual(work_progress.read(link, "files_copied", 6, 25, 1, 10), {"status": "unavailable"})
        os.mkfifo(self.path.with_name("pipe"))
        self.assertEqual(work_progress.read(self.path.with_name("pipe"), "files_copied", 6, 25, 1, 10),
                         {"status": "unavailable"})

    def test_a_directory_in_place_of_stderr_leaves_the_descriptor_closed(self):
        from benchmarks import work_progress
        self.path.mkdir()
        before = len(os.listdir("/proc/self/fd"))
        self.assertEqual(work_progress.read(self.path, "files_copied", 6, 25, 1, 10), {"status": "unavailable"})
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)


class MatrixProgress(unittest.TestCase):
    def fixture(self):
        case = test_matrix_tools.ToolCellTests("runTest")
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def test_a_stop_during_a_copy_keeps_the_observed_work_and_elapsed_time(self):
        fixture = self.fixture()
        binary = fixture.bin / "rcp"
        binary.write_text(binary.read_text().replace("source, destination =", """
import signal
print(json.dumps(dict(schema="rcp-progress-v1", monotonic_ns=time.monotonic_ns(),
                      files_copied=2, hard_links_created=0)), file=sys.stderr, flush=True)
os.kill(os.getppid(), signal.SIGTERM)
time.sleep(15)
source, destination ="""))
        completed = subprocess.run([sys.executable, "-m", "benchmarks.matrix", "--system-telemetry",
                                    *fixture.arguments(("alpha:local",))], capture_output=True, timeout=10)
        self.assertNotEqual(completed.returncode, 0)
        trial = fixture.saved()["cells"][0]["copies"][0]
        self.assertEqual(trial["status"], "interrupted")
        self.assertEqual(trial["work_progress"]["samples"][0][1], 2)
        self.assertGreater(trial["elapsed_seconds"], 0)

    def test_only_instrumented_commands_get_new_ids_and_report_progress(self):
        cases = ("alpha:local", "alpha:local-rcmp", "alpha:local-rlink")
        plain = self.fixture().run_matrix(cells=cases)
        fixture = self.fixture()
        for name in ("rcp", "rlink"):
            binary = fixture.bin / name
            snippet = ('if "--progress-type=json-lines" in sys.argv and "--version" not in sys.argv:\n'
                       '    print(json.dumps(dict(schema="rcp-progress-v1", monotonic_ns=time.monotonic_ns(), '
                       'files_copied=1, hard_links_created=2)), file=sys.stderr)\n')
            binary.write_text(binary.read_text().replace('if "--version" in sys.argv:', snippet + 'if "--version" in sys.argv:'))
        observed = fixture.run_matrix("--system-telemetry", cells=cases)
        for a, b in zip(plain["cells"], observed["cells"], strict=True):
            self.assertEqual(b["outcome"]["status"], "complete")
            if b["scenario"] == "local-rcmp":
                self.assertEqual(a["cell_id"], b["cell_id"])
                self.assertNotIn("work_progress", b["copies"][0])
                continue
            self.assertNotEqual(a["cell_id"], b["cell_id"])
            for trial in b["copies"]:
                progress = trial["work_progress"]
                self.assertEqual(progress["status"], "observed")
                self.assertEqual(progress["samples"][0][1], 1 if b["scenario"] == "local" else 2)
                self.assertIn("--progress-delay=10s", trial["command"])
                self.assertGreaterEqual(progress["command_start_ms"], 0)

    def test_a_historical_binary_keeps_its_command_and_series_identity(self):
        fixture = self.fixture()
        binary = fixture.bin / "rcp"
        binary.write_text(binary.read_text().replace('if "--version" in sys.argv:',
            'if "--progress-type=json-lines" in sys.argv:\n    sys.exit(2)\nif "--version" in sys.argv:'))
        old = fixture.run_matrix("--system-telemetry", cells=("alpha:local",))["cells"][0]
        plain = self.fixture().run_matrix(cells=("alpha:local",))["cells"][0]
        self.assertEqual(old["cell_id"], plain["cell_id"])
        self.assertEqual(old["outcome"]["status"], "complete")
        self.assertEqual(old["copies"][0]["work_progress"], {"status": "unsupported"})
        self.assertNotIn("--progress", old["copies"][0]["command"])
