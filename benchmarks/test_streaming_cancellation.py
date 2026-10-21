"""Real signals reach executor and worker paths in isolated, bounded probes."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from benchmarks import streaming


def signal_probe(scenario):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "nested").mkdir()
        for index in range(8):
            (root / "nested" / str(index)).write_bytes(b"abc")
        expected = streaming.scan_tree(root)
        number = signal.SIGINT if scenario == "sigint" else signal.SIGTERM
        def prior_handler(_number, _frame):
            raise InterruptedError(signal.Signals(_number).name)
        previous = signal.signal(number, prior_handler)
        fired = False
        opened = []
        started, release = threading.Event(), threading.Event()
        original_submit = ThreadPoolExecutor.submit
        original_shutdown = ThreadPoolExecutor.shutdown
        original_wait = streaming.wait
        original_file = streaming._file
        futures = []
        def submit(pool, *args, **kwargs):
            nonlocal fired
            if scenario == "submit" and not fired:
                fired = True
                # model a signal after lock acquisition, before a with statement can own cleanup
                pool._shutdown_lock.acquire()
                os.kill(os.getpid(), number)
                pool._shutdown_lock.release()
            if scenario == "error-drain" and len(futures) == 3:
                raise ValueError("injected submission error")
            future = original_submit(pool, *args, **kwargs)
            futures.append(future)
            return future
        def wait(pending, *args, **kwargs):
            nonlocal fired
            if scenario in ("drain", "sigint") and not fired:
                if not started.wait(3):
                    raise AssertionError("worker did not start")
                fired = True
                os.kill(os.getpid(), number)
                return original_wait(pending, timeout=0)
            return original_wait(pending, *args, **kwargs)
        def shutdown(pool, *args, **kwargs):
            nonlocal fired
            if kwargs.get("cancel_futures"):
                if scenario == "error-drain" and not fired:
                    if not started.wait(3):
                        raise AssertionError("worker did not start")
                    fired = True
                    os.kill(os.getpid(), number)
                original_shutdown(pool, wait=False, cancel_futures=True)
                release.set()
            return original_shutdown(pool, *args, **kwargs)
        def file(parent, name, policy, cancellation):
            cancellation.checkpoint()
            opened.append(name)
            started.set()
            if scenario != "submit":
                if not release.wait(3):
                    raise AssertionError("worker was not released")
                assert os.fstat(parent).st_ino == (root / "nested").stat().st_ino
            cancellation.checkpoint()
            return original_file(parent, name, policy, cancellation)
        def send_from_worker():
            if not started.wait(3):
                raise AssertionError("worker did not start")
            os.kill(os.getpid(), number)
            release.set()
        sender = threading.Thread(target=send_from_worker) if scenario == "worker" else None
        try:
            if sender is not None:
                sender.start()
            with mock.patch.object(streaming, "READ_SIZE", 1), \
                    mock.patch.object(ThreadPoolExecutor, "submit", submit), \
                    mock.patch.object(ThreadPoolExecutor, "shutdown", shutdown), \
                    mock.patch.object(streaming, "wait", wait), \
                    mock.patch.object(streaming, "_file", file):
                proof = streaming.validate_tree(root, expected, workers=2, source=True, inline_bytes=0)
            assert proof.get("interrupted") is True, proof
            assert proof["error"] == signal.Signals(number).name, proof
            assert proof["ok"] is False and proof["complete"] is False, proof
            assert signal.getsignal(number) is prior_handler
            assert all(future.done() for future in futures)
            assert len(opened) <= 2, opened
            assert not any(thread.name.startswith("ThreadPoolExecutor-") for thread in threading.enumerate())
            print(json.dumps(dict(scenario=scenario, proof=proof, started=len(opened),
                                  cancelled=sum(future.cancelled() for future in futures))))
        finally:
            release.set()
            if sender is not None:
                sender.join(3)
            signal.signal(number, previous)


def submission_failure_probe():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "nested").mkdir()
        for name in ("0", "1", "2"):
            (root / "nested" / name).write_bytes(b"abc")
        release = {name: threading.Event() for name in ("0", "1", "2")}
        third_open, third_done = threading.Event(), threading.Event()
        original_file, original_open, original_close = streaming._file, os.open, os.close
        original_adjust = ThreadPoolExecutor._adjust_thread_count
        original_shutdown = ThreadPoolExecutor.shutdown
        parent = None
        adjustments = 0
        evidence = dict(parent_open_at_shutdown=None, worker_done_before_parent_close=None, open_error=None)
        def file(fd, name, *args):
            try:
                if name in ("0", "1") and not release[name].wait(3):
                    raise AssertionError("worker was not released")
                return original_file(fd, name, *args)
            finally:
                if name == "2":
                    third_done.set()
        def opened(path, flags, *args, **kwargs):
            nonlocal parent
            if path == "2":
                third_open.set()
                if not release["2"].wait(3):
                    raise AssertionError("unreturned worker was not released")
            try:
                fd = original_open(path, flags, *args, **kwargs)
            except OSError as error:
                if path == "2":
                    evidence["open_error"] = error.errno
                raise
            if path == "nested":
                parent = fd
            return fd
        def closed(fd):
            if fd == parent:
                evidence["worker_done_before_parent_close"] = third_done.is_set()
            original_close(fd)
        def adjust(pool):
            nonlocal adjustments
            adjustments += 1
            if adjustments != 3:
                return original_adjust(pool)
            release["0"].set()
            if not third_open.wait(3):
                raise AssertionError("existing worker did not start queued task")
            release["1"].set()
            raise RuntimeError("cannot start new thread after enqueue")
        def shutdown(pool, *args, **kwargs):
            if evidence["parent_open_at_shutdown"] is None:
                try:
                    evidence["parent_open_at_shutdown"] = os.fstat(parent).st_ino == (root / "nested").stat().st_ino
                except OSError:
                    evidence["parent_open_at_shutdown"] = False
                release["2"].set()
            return original_shutdown(pool, *args, **kwargs)
        with mock.patch.object(streaming, "_file", file), \
                mock.patch.object(streaming.os, "open", opened), \
                mock.patch.object(streaming.os, "close", closed), \
                mock.patch.object(ThreadPoolExecutor, "_adjust_thread_count", adjust), \
                mock.patch.object(ThreadPoolExecutor, "shutdown", shutdown):
            try:
                streaming.scan_tree(root, workers=3, inline_bytes=0)
            except RuntimeError as error:
                assert str(error) == "cannot start new thread after enqueue", error
            else:
                raise AssertionError("submission failure was lost")
        assert third_done.is_set()
        assert not any(thread.name.startswith("ThreadPoolExecutor-") for thread in threading.enumerate())
        print(json.dumps(evidence))


class StreamingCancellationTests(unittest.TestCase):
    def test_real_signal_at_submit_wait_and_error_drain_is_classified_after_workers_exit(self):
        for scenario in ("submit", "drain", "worker", "error-drain", "sigint"):
            with self.subTest(scenario=scenario):
                result = subprocess.run([sys.executable, "-m", "benchmarks.test_streaming_cancellation", "--probe", scenario],
                                        cwd=Path(__file__).resolve().parents[1], text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                evidence = json.loads(result.stdout)
                self.assertTrue(evidence["proof"]["interrupted"])
                if scenario in ("drain", "error-drain", "sigint"):
                    self.assertGreaterEqual(evidence["cancelled"], 1)

    def test_submit_failure_after_enqueue_drains_unreturned_worker_before_parent_close(self):
        result = subprocess.run([sys.executable, "-m", "benchmarks.test_streaming_cancellation",
                                 "--probe", "submission-failure"],
                                cwd=Path(__file__).resolve().parents[1], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = json.loads(result.stdout)
        self.assertTrue(evidence["parent_open_at_shutdown"], evidence)
        self.assertTrue(evidence["worker_done_before_parent_close"], evidence)
        self.assertIsNone(evidence["open_error"], evidence)

    def test_worker_stops_between_payload_reads_without_reading_remaining_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "file").write_bytes(b"abc")
            parent = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            self.addCleanup(os.close, parent)
            cancellation = streaming._Cancellation()
            reads = []
            actual_fdopen = os.fdopen
            def fdopen(fd, mode):
                stream = actual_fdopen(fd, mode)
                class Stream:
                    def __enter__(self):
                        return self
                    def __exit__(self, *args):
                        stream.close()
                    def read(self, size):
                        reads.append(size)
                        cancellation.stopped = True
                        return stream.read(size)
                return Stream()
            with mock.patch.object(streaming, "READ_SIZE", 1), \
                    mock.patch.object(streaming.os, "fdopen", side_effect=fdopen), \
                    self.assertRaises(streaming._ScanCancelled):
                streaming._file(parent, "file", cancellation=cancellation)
            self.assertEqual(reads, [1])
            os.fstat(parent)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--probe":
        if sys.argv[2] == "submission-failure":
            submission_failure_probe()
        else:
            signal_probe(sys.argv[2])
    else:
        unittest.main()
