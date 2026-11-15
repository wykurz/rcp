"""Remote cells copy through a private SSH endpoint and follow the rules of every other cell."""

import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import signal
import tempfile
import threading
import time
import unittest
from unittest import mock

from benchmarks import matrix, private_sshd, run
from benchmarks.test_matrix import FILEGEN
from benchmarks.test_private_sshd import KEYGEN, SSH, SSHD, TIME, tools

# stands in for rcp. A remote copy must name loopback everywhere and starts one daemon per role
# over `ssh`, each in a session of its own, as sshd does.
RCP = r'''#!/usr/bin/env python3
import os, pathlib, shutil, signal, subprocess, sys, time
if "--version" in sys.argv:
    print("rcp 1")
    sys.exit(0)
arguments, state = sys.argv[1:], pathlib.Path(os.environ["FAKE_STATE"])
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
    wrapper = next(item.partition("=")[2] for item in arguments if item.startswith("--rcpd-path="))
    daemons = [subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no", "127.0.0.1", wrapper, "--role", role],
                                start_new_session=True) for role in ("source", "destination")]
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
print(f"files copied: {sum(len(names) for _, _, names in os.walk(destination))}")
print("files unchanged: 0")
sys.exit(3 if behaviour == "exit" else 0)
'''

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
            'if "-t" in sys.argv:'), ssh=SSH, keygen=KEYGEN, time=TIME)
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
                     "--ssh-keygen", str(self.tools["keygen"]), "--resource-time", str(self.tools["time"])]
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

    def endpoint_directory(self, record, cell=1):
        command = record["cells"][cell]["copies"][0]["command"]
        return Path(next(item.partition("=")[2] for item in command if item.startswith("--rcpd-path="))).parent

    def assert_stopped(self, pids):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and any(Path(f"/proc/{pid}/cmdline").exists() and
                                                  Path(f"/proc/{pid}/cmdline").read_bytes() for pid in pids):
            time.sleep(.05)
        for pid in pids:
            # a process that has ended is gone or a zombie, whose command line reads empty
            self.assertFalse(Path(f"/proc/{pid}/cmdline").exists() and Path(f"/proc/{pid}/cmdline").read_bytes(), pid)

    def test_a_remote_cell_copies_over_loopback_and_measures_each_daemon(self):
        record = self.run_matrix()
        self.assertEqual(record["status"], "complete")
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "remote", "complete")])
        local, remote = record["cells"]
        self.assertNotEqual(local["cell_id"], remote["cell_id"])
        directory = self.endpoint_directory(record)
        fixture = record["file_sets"][0]["generation"]["command"][1] + "/filegen"
        for copy in remote["copies"]:
            self.assertEqual(copy["status"], "ok")
            executable, *options, source, destination = copy["command"]
            self.assertEqual(executable, str(self.bin / "rcp"))
            self.assertEqual(options, ["--summary", "--force-remote", "--bind-ip=127.0.0.1",
                                       f"--rcpd-path={directory}/rcpd"])
            self.assertEqual(source, "127.0.0.1:" + fixture)
            self.assertRegex(destination, r"^127\.0\.0\.1:/.*/alpha/remote/[12]$")
            # the copy command as for a local cell, and each daemon on its own
            self.assertEqual(copy["resources"]["status"], "complete")
            self.assertEqual(sorted(copy["role_resources"]), ["destination", "source"])
            for role in copy["role_resources"].values():
                self.assertEqual((role["status"], role["metrics"]["max_rss_kib"]), ("complete", 2048))
            self.assertNotIn("leftover_processes", copy)
            self.assertEqual(copy["copy_summary"], dict(files_copied=4, files_unchanged=0))
        for copy in local["copies"]:
            self.assertEqual(copy["command"][-2:], [fixture, copy["command"][-1]])
            self.assertNotIn("--force-remote", copy["command"])
            self.assertNotIn("role_resources", copy)
        self.assertEqual(record["context"]["remote"], dict(
            profile="private-sshd-ipv4-loopback-v1", address="127.0.0.1",
            ssh=private_sshd.identity(self.tools["ssh"]), sshd=private_sshd.identity(self.tools["sshd"])))
        # two daemons per copy ran, through the wrapper
        self.assertEqual(len((self.state / "rcpd.pids").read_text().split()), 4)
        # nothing of the endpoint outlives the run, and no key ever reaches the archived results
        self.assertFalse(directory.exists())
        self.assert_stopped([int((self.state / "sshd.pid").read_text())])
        for path in (self.root / "out").rglob("*"):
            if path.is_file():
                self.assertNotIn(b"PRIVATE KEY", path.read_bytes(), path)
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
        self.assertEqual({role["status"] for role in hung["role_resources"].values()}, {"unavailable"})
        self.assert_stopped([int(pid) for pid in (self.state / "rcpd.pids").read_text().split()])
        for copy in record["cells"][1]["copies"]:
            self.assertNotIn("leftover_processes", copy)
            self.assertEqual({role["status"] for role in copy["role_resources"].values()}, {"complete"})

    def test_daemons_that_end_a_moment_after_a_good_copy_are_not_taken_for_hung_ones(self):
        # with and without the resource supervisor: either way the daemons are given time to exit
        for extra in ((), ("--no-resources",)):
            with self.subTest(extra=extra):
                arguments = self.arguments(cells=("alpha:remote",))
                if extra:
                    position = arguments.index("--resource-time")
                    del arguments[position:position + 2]
                with mock.patch.dict(os.environ, {"FAKE_RCP": "alpha:remote=linger", "FAKE_RCPD": "slow-exit"}), \
                        contextlib.redirect_stdout(io.StringIO()):
                    record = matrix.main(arguments)
                cell, = record["cells"]
                self.assertEqual((cell["outcome"]["status"], [copy["status"] for copy in cell["copies"]]),
                                 ("complete", ["ok", "ok"]))
                for copy in cell["copies"]:
                    self.assertNotIn("leftover_processes", copy)
                    if not extra:
                        self.assertEqual({role["status"] for role in copy["role_resources"].values()}, {"complete"})
                # every daemon ended by itself: none was killed
                self.assert_stopped([int(pid) for pid in (self.state / "rcpd.pids").read_text().split()])
                shutil.rmtree(self.root / "out")
                (self.state / "rcpd.pids").unlink()

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

    def test_daemon_figures_that_cannot_be_read_do_not_fail_the_copy(self):
        self.tools["time"].write_text(TIME.replace('stream.write(form + "\\n")', 'stream.write("not the figures")'))
        record = self.run_matrix(cells=("alpha:remote",))
        cell, = record["cells"]
        self.assertEqual((cell["outcome"]["status"], [copy["status"] for copy in cell["copies"]]),
                         ("complete", ["ok", "ok"]))
        for copy in cell["copies"]:
            self.assertEqual({(role["status"], role["metrics"]) for role in copy["role_resources"].values()},
                             {("invalid", None)})

    def test_a_remote_copy_that_fails_fails_its_cell_like_a_local_one(self):
        record = self.run_matrix(cells=("alpha:remote", "alpha:local"), environment={"FAKE_RCP": "alpha:remote=exit"})
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "failed"), ("alpha", "local", "complete")])
        self.assertEqual(record["cells"][0]["outcome"]["message"],
                         "copy 1 failed with exit code 3; see logs/alpha/remote/1/0.stderr.log")
        self.assertEqual({role["status"] for role in record["cells"][0]["copies"][0]["role_resources"].values()},
                         {"unavailable"})

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
        with mock.patch.object(private_sshd, "ROLE_WAIT_SECONDS", .3):
            record = self.run_matrix("--repetitions", "1", cells=("alpha:remote", "beta:remote"),
                                     environment={"FAKE_RCP": "alpha:remote=linger", "FAKE_RCPD": "hang-once"})
        # the copy was good: rcp reported it, and a daemon that would not leave is not rcp failing to copy
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "complete"), ("beta", "remote", "complete")])
        lingering, = record["cells"][0]["copies"]
        self.assertGreaterEqual(lingering["leftover_processes"], 1)
        self.assertNotIn("leftover_processes", record["cells"][1]["copies"][0])
        self.assert_stopped([int(pid) for pid in (self.state / "rcpd.pids").read_text().split()])

    def test_only_known_scenarios_are_selected(self):
        self.assertEqual(matrix.SCENARIOS, ("local", "remote"))
        with self.assertRaisesRegex(ValueError, "unknown scenario: wan"):
            self.run_matrix(cells=("alpha:wan",))
        self.assertFalse((self.root / "out").exists())


if __name__ == "__main__":
    unittest.main()
