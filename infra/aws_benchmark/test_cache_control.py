"""Cache deadlines, broker boundaries, and persisted diagnostics; never touches real caches."""

import contextlib
import io
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import threading
import unittest
from unittest import mock

from benchmarks import cache, run
from benchmarks.harness_test_support import run_fixture
from infra.aws_benchmark import cache_client, worker
from infra.aws_benchmark.common import RESERVE


class Clock:
    def __init__(self):
        self.ns = 100_000_000_000

    def __call__(self):
        return self.ns

    def advance(self, seconds):
        self.ns += int(seconds * 1_000_000_000)


class Connection:
    def __init__(self, value=None, *, chunks=None, peer=None, send_error=None):
        self.parts = list(chunks if chunks is not None else [(json.dumps(value) + "\n").encode()])
        self.peer = os.getuid() if peer is None else peer
        self.sent = bytearray()
        self.timeouts = []
        self.send_error = send_error

    def getsockopt(self, *args):
        return struct.pack("3i", 1, self.peer, os.getgid())

    def settimeout(self, seconds):
        self.timeouts.append(seconds)

    def recv(self, count):
        if not self.parts:
            return b""
        part = self.parts.pop(0)
        if len(part) > count:
            self.parts.insert(0, part[count:])
        return part[:count]

    def sendall(self, data):
        if self.send_error:
            raise self.send_error
        self.sent.extend(data)


class CacheControlTests(unittest.TestCase):
    def request(self, clock, seconds=120):
        return {"version": 1, "operation": "drop", "deadline_ns": clock.ns + seconds * 1_000_000_000}

    def broker(self, request, *, sync=None, drop=None, budget_seconds=3600, peer=None, send_error=None):
        clock = Clock()
        value = request(clock) if callable(request) else request
        connection = Connection(value, peer=peer, send_error=send_error)
        budget = mock.Mock()
        budget.left.return_value = budget_seconds
        history = worker.CacheHistory()
        with mock.patch.object(cache.time, "monotonic_ns", clock), \
                mock.patch.object(worker.os, "sync", side_effect=(lambda: sync(clock)) if sync else None) as synced, \
                mock.patch.object(worker, "Path") as path:
            if drop:
                path.return_value.write_text.side_effect = lambda data: drop(clock)
            worker.handle_cache(connection, budget, os.getuid(), history)
        return connection, history.snapshot(), synced, path, budget

    def test_slow_cache_operation_beyond_old_thirty_seconds_succeeds(self):
        connection, history, synced, path, budget = self.broker(self.request,
            sync=lambda clock: clock.advance(40), drop=lambda clock: clock.advance(15))
        reply = cache.response(json.loads(connection.sent))
        self.assertTrue(reply["ok"])
        self.assertEqual(reply["phases_seconds"], {"sync": 40, "drop": 15})
        synced.assert_called_once()
        path.return_value.write_text.assert_called_once_with("3\n")
        self.assertTrue(all(call.args == (RESERVE,) for call in budget.left.call_args_list))
        self.assertEqual(history["events"][-1], {"operation": 1, "elapsed_seconds": 55, "stage": "reply", "status": "sent", "ok": True})

    def test_expiry_during_sync_never_starts_drop_or_replies_success(self):
        connection, history, _, path, _ = self.broker(lambda clock: self.request(clock, 10),
            sync=lambda clock: clock.advance(11))
        path.assert_not_called()
        self.assertFalse(connection.sent)
        self.assertTrue(any(e.get("error_type") == "TimeoutError" and e["stage"] == "sync" for e in history["events"]))

    def test_worker_budget_and_server_cap_override_untrusted_deadline(self):
        for seconds, elapsed in ((5, 6), (20_000, cache.MAX_SECONDS + 1)):
            with self.subTest(seconds=seconds):
                connection, history, _, path, _ = self.broker(lambda clock: self.request(clock, 30_000),
                    budget_seconds=seconds, sync=lambda clock: clock.advance(elapsed))
                self.assertFalse(connection.sent)
                path.assert_not_called()
                self.assertEqual(history["events"][-1]["status"], "not-confirmed")

    def test_expiry_during_drop_does_not_claim_cancellation_or_success(self):
        connection, history, _, path, _ = self.broker(lambda clock: self.request(clock, 10),
            drop=lambda clock: clock.advance(11))
        path.return_value.write_text.assert_called_once()
        self.assertFalse(connection.sent)
        self.assertEqual(history["events"][-1]["status"], "not-confirmed")
        self.assertTrue(any(e["stage"] == "drop" and e["status"] == "returned" for e in history["events"]))

    def test_malformed_requests_and_wrong_peer_never_sync(self):
        valid = self.request(Clock())
        invalid = [dict(valid, version=True), dict(valid, operation="shell"),
            dict(valid, extra="not allowed"), dict(valid, deadline_ns=True),
            dict(valid, deadline_ns=float("nan")), dict(valid, deadline_ns=float("inf")),
            dict(valid, deadline_ns=2**63), dict(valid, deadline_ns=1)]
        for value in invalid:
            with self.subTest(value=value):
                _, _, synced, path, _ = self.broker(value)
                synced.assert_not_called()
                path.assert_not_called()
        _, _, synced, path, _ = self.broker(self.request, peer=os.getuid() + 1)
        synced.assert_not_called()
        path.assert_not_called()

    def test_broker_error_and_lost_reply_have_independent_trusted_evidence(self):
        def fail(_clock):
            raise OSError(5, "untrusted SECRET text must not enter metadata")
        connection, history, _, _, _ = self.broker(self.request, sync=fail)
        result = cache.response(json.loads(connection.sent))
        self.assertFalse(result["ok"])
        self.assertEqual(result["error_type"], "OSError")
        self.assertEqual(result["errno"], 5)
        self.assertNotIn("SECRET", json.dumps(history))
        connection, history, _, path, _ = self.broker(self.request, send_error=BrokenPipeError())
        path.return_value.write_text.assert_called_once()
        self.assertEqual(history["events"][-1], {"operation": 1, "elapsed_seconds": 0, "stage": "reply", "status": "not-confirmed", "error_type": "BrokenPipeError"})

    def test_history_stays_bounded_and_snapshot_survives_later_writes(self):
        history = worker.CacheHistory()
        history.record(stage="sync", status="started")
        snapshot = history.snapshot()
        for _ in range(300):
            history.record(stage="reply", status="not-confirmed")
        self.assertEqual(len(snapshot["events"]), 1)
        self.assertEqual(len(history.snapshot()["events"]), 256)
        self.assertEqual(history.snapshot()["omitted"], 45)

    def test_fragmented_frames_and_slow_trickle_share_one_deadline(self):
        clock = Clock()
        connection = Connection(chunks=[b'{"version":1,', b'"operation":"drop",', b'"deadline_ns":200000000000}\n'])
        with mock.patch.object(cache.time, "monotonic_ns", clock):
            self.assertEqual(cache.receive(connection, clock.ns + 10_000_000_000, 256)["operation"], "drop")
            original = connection.recv
            connection.parts = [b"{", b" ", b" "]
            def slow(count):
                clock.advance(4)
                return original(count)
            connection.recv = slow
            with self.assertRaises(TimeoutError):
                cache.receive(connection, clock.ns + 10_000_000_000, 256)
        for chunks in ([b"x" * 257], [b"{}"], [b"{}\n{}\n"], [b"drop\n"]):
            with self.subTest(chunks=chunks), self.assertRaises(ValueError):
                cache.receive(Connection(chunks=chunks), cache.deadline_after(1), 256)

    def test_outer_sync_consumes_facade_time_and_stderr_is_bounded(self):
        clock = Clock()
        calls = []
        def execute(argv, **kwargs):
            calls.append((argv[0], kwargs["timeout"]))
            if argv[0] == "sync":
                clock.advance(40)
            else:
                self.assertEqual(int(kwargs["env"][cache.DEADLINE_ENV]), 220_000_000_000)
                kwargs["stderr"].write(b"BEGIN" + b"x" * 100_000 + b"END")
                raise subprocess.CalledProcessError(1, argv)
        report = {}
        with mock.patch.object(cache.time, "monotonic_ns", clock), \
                mock.patch.object(cache.subprocess, "run", side_effect=execute), \
                self.assertRaisesRegex(cache.PreparationError, "BEGIN.*END"):
            cache.prepare(120, report)
        self.assertEqual(calls, [("sync", 120), ("sudo", 80)])
        observation = report["commands"][-1]
        self.assertEqual(observation["returncode"], 1)
        self.assertTrue(observation["stderr"]["truncated"])
        self.assertEqual(len(observation["stderr"]["text"]), cache.DIAGNOSTIC_BYTES)
        self.assertEqual(report["status"], "failed")

    def test_expired_outer_sync_prevents_facade_launch(self):
        clock = Clock()
        report = {}
        def execute(argv, **kwargs):
            clock.advance(121)
        with mock.patch.object(cache.time, "monotonic_ns", clock), \
                mock.patch.object(cache.subprocess, "run", side_effect=execute) as invoked, \
                self.assertRaises(cache.PreparationError):
            cache.prepare(120, report)
        self.assertEqual(invoked.call_count, 1)
        self.assertEqual(report["commands"][0]["error_type"], "TimeoutError")

    def test_cache_cancellation_preserves_type_and_diagnostics_without_retry(self):
        for error in (KeyboardInterrupt(), InterruptedError("SIGTERM"), subprocess.TimeoutExpired(["sudo"], 1)):
            report = {}
            def execute(argv, **kwargs):
                if argv[0] == "sudo":
                    kwargs["stderr"].write(b"cache reply absent")
                    raise error
            with self.subTest(error=type(error).__name__), \
                    mock.patch.object(cache.subprocess, "run", side_effect=execute) as invoked, \
                    self.assertRaises(type(error) if isinstance(error, (KeyboardInterrupt, InterruptedError)) else cache.PreparationError):
                cache.prepare(120, report)
            self.assertEqual(invoked.call_count, 2)
            self.assertIn("cache reply absent", report["commands"][-1]["stderr"]["text"])
            self.assertEqual(report["commands"][-1]["completion"], "unknown")

    def test_cache_failure_persists_stderr_and_prevents_any_copy(self):
        original = subprocess.run
        original_cache = run._prepare_cache
        def execute(argv, **kwargs):
            if argv[0] == "sync":
                return subprocess.CompletedProcess(argv, 0)
            if argv[0] == "sudo":
                kwargs["stderr"].write(b'{"error_type":"TimeoutError","stage":"reply","completion":"unknown"}\n')
                raise subprocess.CalledProcessError(1, argv)
            return original(argv, **kwargs)
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(cache.subprocess, "run", side_effect=execute):
            record, events, summary = run_fixture(Path(directory), cache_prepare=original_cache,
                extra_args=["--cache-timeout", "1200"])
        self.assertEqual(events.count("drop"), 1)
        self.assertNotIn("copy", events)
        trial = record["trials"][0]
        self.assertEqual(trial["failure"]["stage"], "cache")
        self.assertEqual(trial["exit_codes"], [])
        self.assertEqual(trial["cache_preparation"]["timeout_seconds"], 1200)
        self.assertIn("TimeoutError", trial["cache_preparation"]["commands"][-1]["stderr"]["text"])
        self.assertIn("TimeoutError", summary)

    def test_client_and_broker_interoperate_over_real_unix_socket(self):
        with tempfile.TemporaryDirectory() as directory:
            address = str(Path(directory) / "cache.sock")
            budget = mock.Mock()
            budget.left.return_value = 3600
            history = worker.CacheHistory()
            with socket.socket(socket.AF_UNIX) as server:
                server.bind(address)
                server.listen(1)
                def serve():
                    connection, _ = server.accept()
                    with connection:
                        worker.handle_cache(connection, budget, os.getuid(), history)
                stdout, stderr = io.StringIO(), io.StringIO()
                with mock.patch.object(worker.os, "sync"), mock.patch.object(worker, "Path"), \
                        mock.patch.object(cache_client, "SOCKET", address), \
                        mock.patch.object(cache_client.sys, "argv", ["sudo", *cache_client.EXPECTED]), \
                        mock.patch.dict(os.environ, {cache.DEADLINE_ENV: str(cache.deadline_after(5))}), \
                        contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    thread = threading.Thread(target=serve)
                    thread.start()
                    self.assertEqual(cache_client.main(), 0)
                    thread.join(5)
                    self.assertFalse(thread.is_alive())
                self.assertTrue(cache.response(json.loads(stdout.getvalue()))["ok"])
                self.assertEqual(stderr.getvalue(), "")

    def test_client_rejects_missing_deadline_and_unapproved_argv_before_connect(self):
        for argv, env in ((["sudo", "anything"], {}), (["sudo", *cache_client.EXPECTED], {}),
                          (["sudo", *cache_client.EXPECTED], {cache.DEADLINE_ENV: "nan"})):
            with self.subTest(argv=argv, env=env), mock.patch.object(cache_client.sys, "argv", argv), \
                    mock.patch.dict(os.environ, env, clear=True), mock.patch.object(cache_client.socket, "socket") as connect, \
                    contextlib.redirect_stderr(io.StringIO()):
                if argv[-1] == "anything":
                    with self.assertRaises(SystemExit):
                        cache_client.main()
                else:
                    self.assertEqual(cache_client.main(), 1)
                connect.assert_not_called()

    def test_worker_deadline_also_bounds_outer_sync(self):
        clock = Clock()
        report = {}
        def execute(argv, **kwargs):
            self.assertEqual(argv, ["sync"])
            self.assertEqual(kwargs["timeout"], 5)
            clock.advance(6)
        with mock.patch.object(cache.time, "monotonic_ns", clock), \
                mock.patch.dict(os.environ, {cache.WORKER_DEADLINE_ENV: str(clock.ns + 5_000_000_000)}), \
                mock.patch.object(cache.subprocess, "run", side_effect=execute) as invoked, \
                self.assertRaises(cache.PreparationError):
            cache.prepare(1200, report)
        self.assertEqual(invoked.call_count, 1)
        self.assertEqual(report["effective_timeout_seconds"], 5)

    def test_client_eof_or_malformed_reply_fails_without_reconnect(self):
        for chunks in ([b""], [b'{"version":1,"ok":true}\n']):
            connection = mock.MagicMock()
            connection.__enter__.return_value = connection
            connection.recv.side_effect = chunks
            stderr = io.StringIO()
            with self.subTest(chunks=chunks), mock.patch.object(cache_client.socket, "socket", return_value=connection) as opened, \
                    mock.patch.object(cache_client.sys, "argv", ["sudo", *cache_client.EXPECTED]), \
                    mock.patch.dict(os.environ, {cache.DEADLINE_ENV: str(cache.deadline_after(5))}), \
                    contextlib.redirect_stderr(stderr):
                self.assertEqual(cache_client.main(), 1)
            self.assertEqual(opened.call_count, 1)
            self.assertEqual(connection.sendall.call_count, 1)
            self.assertEqual(json.loads(stderr.getvalue())["completion"], "unknown")

    def test_malformed_reply_types_are_rejected_without_traceback(self):
        valid = {"version": 1, "ok": True, "stage": "reply", "phases_seconds": {"sync": 0, "drop": 0}}
        for invalid in (dict(valid, stage=[]), dict(valid, stage={}),
                        dict(valid, phases_seconds={"sync": 10**400, "drop": 0}),
                        dict(valid, phases_seconds={"sync": float("nan"), "drop": 0}),
                        dict(valid, phases_seconds={"sync": True, "drop": 0})):
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                cache.response(invalid)

    def test_cache_timeout_cli_is_independent_and_finite(self):
        for value in ("0", "-1", "nan", "inf", "7201"):
            with self.subTest(value=value), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                run._arguments(["--output", "/unused", "--cache-timeout", value])
        args = run._arguments(["--output", "/unused", "--timeout", "7200", "--cache-timeout", "1200"])
        self.assertEqual((args.timeout, args.cache_timeout), (7200, 1200))


if __name__ == "__main__":
    unittest.main()
