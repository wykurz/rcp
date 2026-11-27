"""Remote cells copy through a private SSH endpoint and follow the rules of every other cell."""

import contextlib
import io
import json
import os
from pathlib import Path
import signal
import tempfile
import threading
import time
import unittest
from unittest import mock

from benchmarks import matrix, private_sshd, run
from benchmarks.test_matrix import FILEGEN, RECORDED, REPORTED
from benchmarks.test_matrix_tools import TOOL
from benchmarks.test_private_sshd import KEYGEN, SSH, SSHD, tools

# what the stand-in rcp reports for each daemon, and what the runner records of it
DAEMONS = dict(source=dict(REPORTED, cpu_time_user_ms=30000, peak_rss_bytes=50 * 1024 * 1024),
               destination=dict(REPORTED, cpu_time_user_ms=20000, peak_rss_bytes=30 * 1024 * 1024))
RECORDED_DAEMONS = dict(source=dict(RECORDED, user_seconds=30.0, max_rss_kib=50 * 1024),
                        destination=dict(RECORDED, user_seconds=20.0, max_rss_kib=30 * 1024))

# stands in for rcp. A remote copy must name loopback everywhere and starts one daemon per role
# over `ssh`, each in a session of its own, as sshd does.
RCP = r'''#!/usr/bin/env python3
import json, os, pathlib, shutil, signal, subprocess, sys, time
old = os.environ.get("FAKE_RCP_BUILD") == "old"
if old and any(item.startswith("--output-format") for item in sys.argv[1:]):
    # a build from before the option existed: its argument parser refuses it, --version or not
    print("error: unexpected argument '--output-format' found", file=sys.stderr)
    sys.exit(2)
if "--version" in sys.argv:
    print("rcp 1")
    sys.exit(0)
arguments, state = sys.argv[1:], pathlib.Path(os.environ["FAKE_STATE"])
if not old and "--output-format=json" not in arguments[:-2]:
    sys.exit("the runner reads the JSON summary")
remote, operands = "--force-remote" in arguments, sys.argv[-2:]
if remote:
    if "--bind-ip=127.0.0.1" not in arguments or not all(item.startswith("127.0.0.1:/") for item in operands):
        sys.exit("a remote copy names loopback for the master and both operands")
    operands = [item.partition(":")[2] for item in operands]
elif any(":" in item for item in operands):
    sys.exit("a local copy has no host")
source, destination = map(pathlib.Path, operands)
plan = dict(item.split("=") for item in os.environ.get("FAKE_RCP", "").split(",") if item)
behaviour = plan.get(source.parent.name + (":remote" if remote else ""), "ok")
if remote:
    daemon = next(item.partition("=")[2] for item in arguments if item.startswith("--rcpd-path="))
    started = state / "rcpd.pids"
    before = len(started.read_text().split()) if started.exists() else 0
    daemons = [subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no", "127.0.0.1", daemon, "--role", role],
                                start_new_session=True) for role in ("source", "destination")]
    # rcp goes on only once both daemons have told it that they are up
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and (not started.exists() or len(started.read_text().split()) < before + 2):
        time.sleep(.01)
    if behaviour == "hang":
        time.sleep(300)
    if behaviour != "linger" and any(daemon.wait() for daemon in daemons):
        sys.exit("a daemon could not be started")
    if behaviour in ("killsshd", "breaksshd") and not (state / "killed").exists():
        (state / "killed").write_text("")
        if behaviour == "breaksshd":
            (state / "break-sshd").write_text("")
        os.kill(int((state / "sshd.pid").read_text()), signal.SIGKILL)
shutil.copytree(source, destination)
if old:
    # the text summary is all such a build has
    print("files copied: %d\nfiles unchanged: 0" % sum(len(names) for _, _, names in os.walk(destination)))
    sys.exit(0)
summary = dict(REPORTED, files_copied=sum(len(names) for _, _, names in os.walk(destination)), files_unchanged=0)
if remote and behaviour != "nodaemons":
    # what each daemon sent the master about itself, with the host it ran on
    summary["remote"] = {role: dict(figures, host="127.0.0.1") for role, figures in DAEMONS.items()}
    if behaviour == "onedaemon":
        del summary["remote"]["source"]["minor_page_faults"]
    if behaviour == "blinddaemon":
        # a daemon that is named without any statistics
        summary["remote"]["source"] = dict(host="127.0.0.1")
print(json.dumps(summary))
sys.exit(3 if behaviour == "exit" else 0)
'''.replace("REPORTED", repr(REPORTED)).replace("DAEMONS", repr(DAEMONS))

# stands in for rcpd: records that it ran, and can outlive the copy that started it
RCPD = r'''#!/bin/sh
case "$1" in --version) echo "rcpd 1"; exit 0 ;; esac
echo $$ >> "$FAKE_STATE/rcpd.pids"
if [ "$FAKE_RCPD" = hang-once ] && [ ! -e "$FAKE_STATE/hung" ]; then
  : > "$FAKE_STATE/hung"
  exec sleep 300
fi
if [ "$FAKE_RCPD" = slow-exit ]; then
  sleep 0.4
fi
'''


class RemoteCellTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.bin = self.root / "bin"
        tools(self.bin, filegen=FILEGEN, rcp=RCP, rcpd=RCPD)
        self.tools = tools(self.root / "tools", sshd=SSHD.replace(
            'if "-t" in sys.argv:', 'if "-t" in sys.argv and (state / "break-sshd").exists():\n    sys.exit(1)\n'
            'if "-t" in sys.argv:'), ssh=SSH, keygen=KEYGEN)
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(json.dumps(dict(schema_version=1, cases=[
            dict(id=name, directory_widths=[2], files_per_leaf=2, file_size_bytes=3) for name in ("alpha", "beta")],
            variants=[dict(id="rcp-default", tool="rcp", args=["--summary"], processes=1)])))
        for side in ("source", "destination"):
            (self.root / side).mkdir()
        environment = mock.patch.dict(os.environ, {"FAKE_STATE": str(self.state)})
        environment.start()
        self.addCleanup(environment.stop)

    def arguments(self, *extra, cells):
        arguments = ["--manifest", str(self.manifest), "--bin-dir", str(self.bin),
                     "--source-root", str(self.root / "source"), "--destination-root", str(self.root / "destination"),
                     "--cache", "uncontrolled", "--repetitions", "2", "--output", str(self.root / "out"),
                     "--sshd", str(self.tools["sshd"]), "--ssh", str(self.tools["ssh"]),
                     "--ssh-keygen", str(self.tools["keygen"])]
        for cell in cells:
            arguments += ["--cell", cell]
        return [*arguments, *extra]

    def run_matrix(self, *extra, cells=("alpha:local", "alpha:remote"), environment=None):
        with mock.patch.dict(os.environ, environment or {}), contextlib.redirect_stdout(io.StringIO()):
            record = matrix.main(self.arguments(*extra, cells=cells))
        saved = json.loads((self.root / "out/results.json").read_text())
        self.assertEqual(saved, json.loads(json.dumps(record)))
        matrix.validate(saved)
        return saved

    def outcomes(self, record):
        return [(cell["case_id"], cell["scenario"], cell["outcome"]["status"]) for cell in record["cells"]]

    @contextlib.contextmanager
    def endpoint_directories(self):
        """Collect the private directories that endpoints make while the block runs."""
        made, mkdtemp = [], tempfile.mkdtemp
        def recording(**options):
            path = mkdtemp(**options)
            if options.get("prefix") == "rcp-bench-ssh-":
                made.append(Path(path))
            return path
        with mock.patch.object(private_sshd.tempfile, "mkdtemp", recording):
            yield made

    def assert_stopped(self, pids):
        def running(pid):
            # Exited processes have no proc entry; zombies have an empty command line.
            try:
                return bool(Path(f"/proc/{pid}/cmdline").read_bytes())
            except FileNotFoundError:
                return False

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and any(running(pid) for pid in pids):
            time.sleep(.05)
        for pid in pids:
            self.assertFalse(running(pid), pid)

    def test_assert_stopped_accepts_a_process_that_disappears_between_checks(self):
        # Even if a preceding existence check sees the process, it can disappear before open.
        with (mock.patch.object(Path, "exists", return_value=True),
              mock.patch.object(Path, "read_bytes", side_effect=FileNotFoundError)):
            self.assert_stopped([123])

    def test_a_remote_cell_copies_over_loopback_and_records_what_each_process_reports(self):
        with self.endpoint_directories() as made:
            record = self.run_matrix()
        self.assertEqual(record["status"], "complete")
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "remote", "complete")])
        local, remote = record["cells"]
        self.assertNotEqual(local["cell_id"], remote["cell_id"])
        directory, = made
        fixture = record["file_sets"][0]["generation"]["command"][1] + "/filegen"
        for copy in remote["copies"]:
            self.assertEqual(copy["status"], "ok")
            executable, *options, source, destination = copy["command"]
            self.assertEqual(executable, str(self.bin / "rcp"))
            # the daemon is the build under test itself: nothing stands between rcp and it
            self.assertEqual(options, ["--summary", "--output-format=json", "--force-remote", "--bind-ip=127.0.0.1",
                                       f"--rcpd-path={self.bin}/rcpd"])
            self.assertEqual(source, "127.0.0.1:" + fixture)
            self.assertRegex(destination, r"^127\.0\.0\.1:/.*/alpha/remote/[12]$")
            # the rcp process as for a local cell, and each daemon as it reported to the master
            self.assertEqual(copy["resources"], dict(status="complete", metrics=RECORDED))
            self.assertEqual(copy["role_resources"], {role: dict(status="complete", metrics=metrics)
                                                      for role, metrics in RECORDED_DAEMONS.items()})
            self.assertNotIn("leftover_processes", copy)
            self.assertEqual(copy["copy_summary"], dict(files_copied=4, files_unchanged=0))
        for copy in local["copies"]:
            self.assertEqual(copy["command"][-2:], [fixture, copy["command"][-1]])
            self.assertNotIn("--force-remote", copy["command"])
            self.assertNotIn("role_resources", copy)
        self.assertEqual(record["context"]["remote"], dict(
            profile="private-sshd-ipv4-loopback-v1", address="127.0.0.1",
            ssh=private_sshd.identity(self.tools["ssh"]), sshd=private_sshd.identity(self.tools["sshd"])))
        self.assertEqual(record["context"]["resources"], dict(policy="rcp-output-json-v1"))
        # two daemons per copy ran
        self.assertEqual(len((self.state / "rcpd.pids").read_text().split()), 4)
        # nothing of the endpoint outlives the run, and no key ever reaches the archived results
        self.assertFalse(directory.exists())
        self.assert_stopped([int((self.state / "sshd.pid").read_text())])
        for path in (self.root / "out").rglob("*"):
            if path.is_file():
                self.assertNotIn(b"PRIVATE KEY", path.read_bytes(), path)
                self.assertNotIn(b"RCP_BENCH_ENDPOINT", path.read_bytes(), path)
        self.assertIn("Server listening", (self.root / "out/logs/sshd.log").read_text())
        self.assertEqual(list((self.root / "destination").iterdir()), [])
        self.assertIn("| alpha | remote | complete |", (self.root / "out/summary.md").read_text())

    def test_an_endpoint_that_cannot_start_fails_only_the_remote_cells(self):
        record = self.run_matrix(cells=("alpha:remote", "alpha:local", "beta:remote"),
                                 environment={"FAKE_SSH": "nologin"})
        self.assertEqual(record["status"], "partial")
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "failed"), ("alpha", "local", "complete"),
                                                 ("beta", "remote", "failed")])
        for cell in (record["cells"][0], record["cells"][2]):
            self.assertEqual(cell["outcome"]["stage"], "transport")
            self.assertEqual(cell["outcome"]["message"], "the private ssh endpoint could not be started: the private "
                             "sshd accepted a connection but the login failed: This account is currently not available.")
            self.assertEqual(cell["copies"], [])
        self.assertNotIn("remote", record["context"])
        # a file set with nothing left to measure is not generated at all
        self.assertEqual([item["generation"]["status"] for item in record["file_sets"]], ["ok", "not_run"])

    def test_an_endpoint_that_cannot_start_leaves_a_file_set_of_remote_copies_and_their_tools_ungenerated(self):
        tools(self.bin, rcmp=TOOL, rrm=TOOL)
        record = self.run_matrix(cells=("alpha:remote", "alpha:local", "beta:remote", "beta:remote-rcmp",
                                        "beta:remote-rrm"), environment={"FAKE_SSH": "nologin"})
        self.assertEqual(self.outcomes(record), [
            ("alpha", "remote", "failed"), ("alpha", "local", "complete"), ("beta", "remote", "failed"),
            ("beta", "remote-rcmp", "failed"), ("beta", "remote-rrm", "failed")])
        # a tool after remote copies needs those copies: nothing of this file set can be measured,
        # so it is not generated either
        self.assertEqual([item["generation"]["status"] for item in record["file_sets"]], ["ok", "not_run"])
        copy = record["cells"][2]["outcome"]
        self.assertEqual(copy["stage"], "transport")
        for cell in record["cells"][3:]:
            self.assertEqual((cell["outcome"]["stage"], cell["outcome"]["message"], cell["copies"]),
                             ("copy", "the remote copy that this cell follows did not complete: " + copy["message"], []))

    def test_a_run_without_remote_cells_never_touches_ssh(self):
        record = self.run_matrix("--sshd", str(self.root / "absent"), cells=("alpha:local",))
        self.assertEqual(record["status"], "complete")
        self.assertNotIn("remote", record["context"])
        self.assertFalse((self.state / "ssh.calls").exists())
        self.assertFalse((self.state / "sshd.launches").exists())

    def test_a_hung_remote_copy_times_out_and_none_of_its_daemons_is_left_running(self):
        record = self.run_matrix("--timeout", "6", cells=("alpha:remote", "beta:remote"),
                                 environment={"FAKE_RCP": "alpha:remote=hang", "FAKE_RCPD": "hang-once"})
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "timed_out"), ("beta", "remote", "complete")])
        hung = record["cells"][0]["copies"][0]
        self.assertEqual(record["cells"][0]["outcome"]["message"], "copy 1 did not finish within 6 s")
        # the daemon was started by sshd in a session of its own, outside the copy's process group
        self.assertGreaterEqual(hung["leftover_processes"], 1)
        # a copy that never finished reported nothing about any process
        self.assertNotIn("resources", hung)
        self.assertNotIn("role_resources", hung)
        self.assert_stopped([int(pid) for pid in (self.state / "rcpd.pids").read_text().split()])
        for copy in record["cells"][1]["copies"]:
            self.assertNotIn("leftover_processes", copy)
            self.assertEqual({role["status"] for role in copy["role_resources"].values()}, {"complete"})

    def test_daemons_that_end_a_moment_after_a_good_copy_are_not_taken_for_hung_ones(self):
        record = self.run_matrix(cells=("alpha:remote",),
                                 environment={"FAKE_RCP": "alpha:remote=linger", "FAKE_RCPD": "slow-exit"})
        cell, = record["cells"]
        self.assertEqual((cell["outcome"]["status"], [copy["status"] for copy in cell["copies"]]),
                         ("complete", ["ok", "ok"]))
        for copy in cell["copies"]:
            self.assertNotIn("leftover_processes", copy)
        # every daemon ended by itself: none was killed
        self.assert_stopped([int(pid) for pid in (self.state / "rcpd.pids").read_text().split()])

    def test_a_listener_that_dies_between_copies_is_started_again(self):
        record = self.run_matrix(cells=("alpha:remote",), environment={"FAKE_RCP": "alpha:remote=killsshd"})
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "complete")])
        first, second = record["cells"][0]["copies"]
        self.assertNotIn("endpoint_restarted", first)
        self.assertIs(second["endpoint_restarted"], True)
        launches = (self.state / "sshd.launches").read_text().split()
        self.assertEqual((len(launches), launches[0]), (2, launches[1]))

    def test_a_listener_that_cannot_be_started_again_fails_the_remote_cells_after_it(self):
        record = self.run_matrix(cells=("alpha:remote", "alpha:local", "beta:remote"),
                                 environment={"FAKE_RCP": "alpha:remote=breaksshd"})
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "failed"), ("alpha", "local", "complete"),
                                                 ("beta", "remote", "failed")])
        broken = record["cells"][0]
        self.assertEqual([copy["status"] for copy in broken["copies"]], ["ok"])
        self.assertEqual((broken["outcome"]["stage"], broken["outcome"]["message"]), (
            "transport", "the private ssh endpoint stopped and could not be started again before copy 2: "
                         "sshd rejected its configuration: no output"))
        self.assertIsNone(broken["summary"])
        self.assertEqual((record["cells"][2]["outcome"]["stage"], record["cells"][2]["outcome"]["message"]), (
            "transport", "the private ssh endpoint stopped and could not be started again: "
                         "sshd rejected its configuration: no output"))
        self.assertEqual(record["cells"][2]["copies"], [])
        # a file set with nothing left to measure is not generated at all
        self.assertEqual([item["generation"]["status"] for item in record["file_sets"]], ["ok", "not_run"])

    def test_a_summary_without_a_daemon_s_figures_does_not_fail_the_copy(self):
        record = self.run_matrix(cells=("alpha:remote", "beta:remote"), environment={
            "FAKE_RCP": "alpha:remote=nodaemons,beta:remote=onedaemon"})
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "complete"), ("beta", "remote", "complete")])
        for copy in record["cells"][0]["copies"]:
            self.assertEqual(copy["resources"]["status"], "complete")
            self.assertEqual({(role["status"], role["metrics"]) for role in copy["role_resources"].values()},
                             {("unavailable", None)})
        # one daemon's figures being unusable costs that daemon's, not the other's
        for copy in record["cells"][1]["copies"]:
            self.assertEqual((copy["role_resources"]["source"]["status"], copy["role_resources"]["source"]["error"]),
                             ("unavailable", "minor_page_faults is missing or is not a count"))
            self.assertEqual(copy["role_resources"]["destination"],
                             dict(status="complete", metrics=RECORDED_DAEMONS["destination"]))

    def test_a_daemon_named_without_statistics_has_none_recorded(self):
        record = self.run_matrix(cells=("alpha:remote",), environment={"FAKE_RCP": "alpha:remote=blinddaemon"})
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "complete")])
        for copy in record["cells"][0]["copies"]:
            self.assertEqual(copy["role_resources"]["source"], dict(
                status="unavailable", metrics=None, error="the summary has no statistics for this process"))
            self.assertEqual(copy["role_resources"]["destination"],
                             dict(status="complete", metrics=RECORDED_DAEMONS["destination"]))

    def test_a_remote_copy_that_fails_fails_its_cell_like_a_local_one(self):
        record = self.run_matrix(cells=("alpha:remote", "alpha:local"), environment={"FAKE_RCP": "alpha:remote=exit"})
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "failed"), ("alpha", "local", "complete")])
        self.assertEqual(record["cells"][0]["outcome"]["message"],
                         "copy 1 failed with exit code 3; see 0.stdout.log and 0.stderr.log in logs/alpha/remote/1")
        # what a failed copy printed is not read
        for key in ("copy_summary", "resources", "role_resources"):
            self.assertNotIn(key, record["cells"][0]["copies"][0])

    def test_a_build_without_the_json_summary_copies_remotely_without_figures(self):
        record = self.run_matrix(environment={"FAKE_RCP_BUILD": "old"})
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "remote", "complete")])
        self.assertEqual(record["context"]["summary_format"], "text")
        self.assertNotIn("resources", record["context"])
        for copy in record["cells"][1]["copies"]:
            _, *options, _, _ = copy["command"]
            self.assertEqual(options, ["--summary", "--force-remote", "--bind-ip=127.0.0.1",
                                       f"--rcpd-path={self.bin}/rcpd"])
            self.assertEqual(copy["copy_summary"], dict(files_copied=4, files_unchanged=0))
            self.assertNotIn("resources", copy)
            self.assertNotIn("role_resources", copy)

    def test_an_unforeseen_error_in_a_remote_cell_ends_its_daemons_before_the_next_cell(self):
        bound, reap, failed, reaped = matrix._bound_logs, private_sshd.Endpoint.reap, [], []

        def failing(directory):
            # the copy has ended and one of its daemons is still running
            if str(directory).endswith("alpha/remote/1") and not failed:
                failed.append(directory)
                raise RuntimeError("nobody foresaw this")
            return bound(directory)

        def recording(endpoint, grace=0):
            started = (self.root / "out/logs/beta").exists()
            reaped.append((grace, reap(endpoint, grace), started))
            return reaped[-1][1]
        with mock.patch.object(matrix, "_bound_logs", failing), \
                mock.patch.object(private_sshd.Endpoint, "reap", recording):
            record = self.run_matrix("--repetitions", "1", cells=("alpha:remote", "beta:local"),
                                     environment={"FAKE_RCP": "alpha:remote=linger", "FAKE_RCPD": "hang-once"})
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "failed"), ("beta", "local", "complete")])
        self.assertEqual((record["cells"][0]["outcome"]["stage"], record["cells"][0]["outcome"]["message"]),
                         ("harness", "unexpected RuntimeError: nobody foresaw this"))
        # ended at once, and before the next cell was measured: a daemon must not work through it
        grace, ended, started = reaped[0]
        self.assertEqual((grace, started), (0, False))
        self.assertGreaterEqual(ended, 1)
        self.assert_stopped([int(pid) for pid in (self.state / "rcpd.pids").read_text().split()])

    def test_a_stop_signal_while_daemons_are_ended_is_not_taken_for_a_failed_cleanup(self):
        class Stopped:
            def reap(self, grace=0):
                raise InterruptedError("SIGTERM")

        class Broken:
            def reap(self, grace=0):
                raise PermissionError("no access")
        with self.assertRaises(InterruptedError):
            matrix._end_daemons(Stopped(), {}, True)
        # ending daemons is housekeeping: when it fails otherwise, that is not the copy's failure
        item = {}
        matrix._end_daemons(Broken(), item, False)
        self.assertEqual(item, {})

    def test_a_stop_signal_ends_the_endpoint_and_removes_its_keys(self):
        calls, made, mkdtemp = [], [], tempfile.mkdtemp
        def prepare(*args, **kwargs):
            calls.append(kwargs)
            if len(calls) == 2:
                raise InterruptedError("SIGTERM")
        def recording(**options):
            path = mkdtemp(**options)
            if options.get("prefix") == "rcp-bench-ssh-":
                made.append(path)
            return path
        with mock.patch.object(run, "_prepare_cache", side_effect=prepare), \
                mock.patch.object(private_sshd.tempfile, "mkdtemp", recording), self.assertRaises(InterruptedError):
            matrix.main(self.arguments(cells=("alpha:remote",)))
        record = matrix.validate(json.loads((self.root / "out/results.json").read_text()))
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "interrupted")])
        self.assertEqual([copy["status"] for copy in record["cells"][0]["copies"]], ["ok", "interrupted"])
        self.assertEqual(len(made), 1)
        self.assertFalse(Path(made[0]).exists())
        self.assert_stopped([int((self.state / "sshd.pid").read_text())])

    def test_a_final_save_that_fails_still_ends_the_endpoint_and_removes_its_keys(self):
        made, mkdtemp, persist = [], tempfile.mkdtemp, matrix._persist
        def recording(**options):
            path = mkdtemp(**options)
            if options.get("prefix") == "rcp-bench-ssh-":
                made.append(path)
            return path
        def failing(output, record, sampler=None):
            # the output directory fills up at the very end: the saves during the run still worked
            if record["status"] != "running":
                raise PermissionError("the output directory cannot be written")
            return persist(output, record, sampler)
        with mock.patch.object(matrix, "_persist", failing), \
                mock.patch.object(private_sshd.tempfile, "mkdtemp", recording), \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaises(PermissionError):
            matrix.main(self.arguments(cells=("alpha:remote",)))
        # sshd runs in a session of its own and would outlive the runner, with its key beside it
        self.assertEqual(len(made), 1)
        self.assertFalse(Path(made[0]).exists())
        self.assert_stopped([int((self.state / "sshd.pid").read_text())])

    def test_a_user_without_an_account_fails_the_remote_cells_and_not_the_run(self):
        with mock.patch.object(private_sshd.pwd, "getpwuid", side_effect=KeyError("uid")):
            record = self.run_matrix(cells=("alpha:local", "alpha:remote"))
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "remote", "failed")])
        self.assertEqual(record["cells"][1]["outcome"], dict(status="failed", stage="transport", message=(
            "the private ssh endpoint could not be started: the current user has no account entry, "
            "so sshd cannot log it in")))

    def test_a_real_stop_signal_during_a_remote_copy_ends_its_daemons_and_keeps_the_results(self):
        def stop(_number, _frame):
            raise InterruptedError("SIGTERM")
        self.addCleanup(signal.signal, signal.SIGTERM, signal.signal(signal.SIGTERM, stop))
        waiting = threading.Event()
        def signal_once_a_daemon_runs():
            # only while the run under test is waiting for it: a signal sent later would end
            # whatever the test process happened to be doing by then
            while not waiting.wait(.02):
                if (self.state / "hung").exists():
                    os.kill(os.getpid(), signal.SIGTERM)
                    return
        self.addCleanup(waiting.set)
        sender = threading.Thread(target=signal_once_a_daemon_runs, daemon=True)
        made, mkdtemp = [], tempfile.mkdtemp
        def recording(**options):
            path = mkdtemp(**options)
            if options.get("prefix") == "rcp-bench-ssh-":
                made.append(path)
            return path
        with mock.patch.dict(os.environ, {"FAKE_RCP": "alpha:remote=hang", "FAKE_RCPD": "hang-once"}), \
                mock.patch.object(private_sshd.tempfile, "mkdtemp", recording), \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(InterruptedError, "SIGTERM"):
            sender.start()
            try:
                matrix.main(self.arguments("--timeout", "60", cells=("alpha:remote", "beta:local")))
            finally:
                waiting.set()
        sender.join()
        record = matrix.validate(json.loads((self.root / "out/results.json").read_text()))
        self.assertEqual((record["status"], record["error"]), ("failed", "SIGTERM"))
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "interrupted"), ("beta", "local", "not_run")])
        self.assertEqual(record["cells"][0]["outcome"]["message"], "run stopped during copy 1 of 2")
        # the daemon that was at work when the run stopped is gone, with sshd and the keys
        self.assert_stopped([int(pid) for pid in (self.state / "rcpd.pids").read_text().split()])
        self.assert_stopped([int((self.state / "sshd.pid").read_text())])
        self.assertEqual(len(made), 1)
        self.assertFalse(Path(made[0]).exists())

    def test_a_daemon_still_running_when_its_time_to_exit_is_up_is_ended_and_counted(self):
        with mock.patch.object(private_sshd, "EXIT_SECONDS", .3):
            record = self.run_matrix("--repetitions", "1", cells=("alpha:remote", "beta:remote"),
                                     environment={"FAKE_RCP": "alpha:remote=linger", "FAKE_RCPD": "hang-once"})
        # the copy was good: rcp reported it, and a daemon that would not leave is not rcp failing to copy
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "complete"), ("beta", "remote", "complete")])
        lingering, = record["cells"][0]["copies"]
        self.assertGreaterEqual(lingering["leftover_processes"], 1)
        self.assertNotIn("leftover_processes", record["cells"][1]["copies"][0])
        self.assert_stopped([int(pid) for pid in (self.state / "rcpd.pids").read_text().split()])

    def test_the_other_tools_follow_remote_copies_as_they_follow_local_ones(self):
        tools(self.bin, rcmp=TOOL, rlink=TOOL, rrm=TOOL)
        seen, execute = [], run.execute_commands
        def note(commands, *args, **kwargs):
            seen.append(Path(commands[0][0]).name)
            return execute(commands, *args, **kwargs)
        cells = ("alpha:local", "alpha:local-rrm", "alpha:remote", "alpha:remote-rcmp", "alpha:remote-rlink",
                 "alpha:remote-rrm")
        with mock.patch.object(run, "execute_commands", side_effect=note):
            record = self.run_matrix(cells=cells)
        self.assertEqual((record["status"], {status for _, _, status in self.outcomes(record)}),
                         ("complete", {"complete"}))
        # each cell's tools follow its own copies
        self.assertEqual(seen, ["rcp", "rrm"] * 2 + ["rcp", "rcmp", "rlink", "rrm"] * 2)
        by_scenario = {cell["scenario"]: cell for cell in record["cells"]}
        remote = by_scenario["remote"]["copies"]
        for scenario in ("remote-rcmp", "remote-rlink", "remote-rrm"):
            runs = by_scenario[scenario]["copies"]
            self.assertEqual([item["follows_trial_index"] for item in runs], [copy["trial_index"] for copy in remote])
            self.assertEqual([item["resources"]["status"] for item in runs], ["complete", "complete"])
            for item in runs:
                # a tool works on the copy where it is, with plain paths: no host, no daemon, no ssh
                self.assertFalse(any("127.0.0.1" in argument for argument in item["command"]))
                self.assertNotIn("role_resources", item)
        self.assertIn("/alpha/remote/1", by_scenario["remote-rrm"]["copies"][0]["command"][-1])
        self.assertEqual(by_scenario["remote-rrm"]["variant"], by_scenario["local-rrm"]["variant"])
        self.assertNotEqual(by_scenario["remote-rrm"]["cell_id"], by_scenario["local-rrm"]["cell_id"])
        self.assertEqual(list((self.root / "destination").iterdir()), [])
        self.assertNotIn("rrm_fallback", record)

    def test_only_known_scenarios_are_selected(self):
        self.assertEqual(matrix.SCENARIOS, ("local", "remote", "nfs", "memory", "local-rcmp", "local-rlink", "local-rrm",
                                            "remote-rcmp", "remote-rlink", "remote-rrm", "nfs-rcmp", "nfs-rlink",
                                            "nfs-rrm", "memory-rcmp", "memory-rlink", "memory-rrm"))
        with self.assertRaisesRegex(ValueError, "unknown scenario: wan"):
            self.run_matrix(cells=("alpha:wan",))
        self.assertFalse((self.root / "out").exists())


if __name__ == "__main__":
    unittest.main()
