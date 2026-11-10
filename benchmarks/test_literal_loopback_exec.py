"""Pure/fake-process tests of the private SSH inner owner; no real executions."""
import copy
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from benchmarks import literal_loopback as contract
from benchmarks import literal_loopback_exec as executor


class InnerTests(unittest.TestCase):
    def setUp(self):
        self.plan = dict(schema=contract.SCHEMA, id="1" * 32,
                         unit=contract.unit_name("1" * 32), account="runner", uid=1001, gid=1001,
                         host_netns="net:[1]", binary_sha256=dict(rcp="a" * 64, rcpd="b" * 64))
        self.status = dict(NoNewPrivs="1", Groups="1001", **{name: "0" for name in
                           ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")})
        self.proof = executor.identity(self.plan, uid=1001, gid=1001, status=self.status,
                                       netns="net:[2]", interfaces=["lo"])

    def test_private_identity_rejects_root_caps_host_network_and_veth(self):
        good = dict(uid=1001, gid=1001, status=self.status, netns="net:[2]", interfaces=["lo"])
        for edit in (dict(uid=0), dict(gid=0), dict(netns="net:[1]"),
                     dict(interfaces=["lo", "eth0"]),
                     dict(status=dict(self.status, NoNewPrivs="0")),
                     dict(status=dict(self.status, CapBnd="1")),
                     dict(status=dict(self.status, Groups="1001 27")),
                     dict(status=dict(self.status, Groups="not-a-gid"))):
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                executor.identity(self.plan, **dict(good, **edit))

    def test_nss_name_and_uid_both_must_resolve_to_private_view(self):
        record = SimpleNamespace(pw_name="runner", pw_uid=1001, pw_gid=1001,
                                 pw_dir=str(executor.HOME), pw_shell="/bin/sh")
        executor.verify_account(self.plan, record, record)
        for name in ("pw_name", "pw_uid", "pw_gid", "pw_dir", "pw_shell"):
            bad = copy.copy(record)
            setattr(bad, name, "wrong")
            with self.subTest(field=name), self.assertRaises(ValueError):
                executor.verify_account(self.plan, record, bad)

    def test_child_environment_does_not_inherit_credentials_or_agent(self):
        with patch.dict(os.environ, AWS_SECRET_ACCESS_KEY="never-copy", SSH_AUTH_SOCK="/run/agent",
                        LD_PRELOAD="/bad"):
            env = executor.clean_environment(self.plan)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", env)
        self.assertNotIn("SSH_AUTH_SOCK", env)
        self.assertNotIn("LD_PRELOAD", env)
        self.assertEqual(env["HOME"], str(executor.HOME))

    def test_private_entry_point_rejects_an_owner_command_without_spawning(self):
        with patch.object(executor.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "private inner/role mode"):
                executor.main(["--native-smoke"])
        run.assert_not_called()

    def fake_trial(self, failure=False):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        home = Path(temporary.name)
        server = SimpleNamespace(code=None, terminated=0, waited=0, killed=0)
        server.poll = lambda: server.code
        def terminate():
            server.terminated += 1
        def wait(timeout):
            server.waited += 1
            server.code = 0
            return 0
        server.terminate, server.wait = terminate, wait
        server.kill = lambda: setattr(server, "killed", server.killed + 1)
        copy_calls = []
        def fake_run(argv, plan, timeout, **kwargs):
            if argv == contract.copy_argv():
                copy_calls.append(argv)
                if failure:
                    raise executor.subprocess.CalledProcessError(1, argv)
                for role in ("source", "destination"):
                    executor.save_new(home / ("role-" + role + ".json"), dict(
                        role=role, identity=self.proof, pid=123,
                        binary_sha256=self.plan["binary_sha256"]["rcpd"]))
            return SimpleNamespace(stdout=b"same-protocol-identity")
        return home, server, fake_run, copy_calls

    def test_copy_is_attempted_once_and_private_server_is_reaped(self):
        home, server, fake_run, calls = self.fake_trial()
        with patch.object(executor, "HOME", home), patch.object(executor, "read_plan", return_value=self.plan),              patch.object(executor, "observed_identity", return_value=self.proof),              patch.object(executor, "_run", side_effect=fake_run),              patch.object(executor.subprocess, "Popen", return_value=server):
            result = executor.inner_main("not-used-by-fake")
        self.assertEqual(result, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual((server.terminated, server.waited), (1, 1))
        self.assertTrue((home / "copy-intent.json").exists())
        record = json.loads((home / "inner-result.json").read_text())
        self.assertEqual(record["status"], "copy-succeeded")
        self.assertEqual(set(record["roles"]), {"source", "destination"})

    def test_failed_copy_is_not_retried_and_failure_is_retained(self):
        home, server, fake_run, calls = self.fake_trial(failure=True)
        with patch.object(executor, "HOME", home), patch.object(executor, "read_plan", return_value=self.plan),              patch.object(executor, "observed_identity", return_value=self.proof),              patch.object(executor, "_run", side_effect=fake_run),              patch.object(executor.subprocess, "Popen", return_value=server):
            result = executor.inner_main("not-used-by-fake")
        self.assertEqual(result, 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(json.loads((home / "inner-result.json").read_text())["failure"]["type"],
                         "CalledProcessError")

    def test_teardown_failure_preserves_copy_error_and_cannot_report_success(self):
        for copy_failure in (False, True):
            with self.subTest(copy_failure=copy_failure):
                home, server, fake_run, calls = self.fake_trial(failure=copy_failure)
                server.terminate = lambda: (_ for _ in ()).throw(PermissionError("cannot stop"))
                with patch.object(executor, "HOME", home), patch.object(executor, "read_plan", return_value=self.plan), \
                     patch.object(executor, "observed_identity", return_value=self.proof), \
                     patch.object(executor, "_run", side_effect=fake_run), \
                     patch.object(executor.subprocess, "Popen", return_value=server):
                    self.assertEqual(executor.inner_main("fake"), 1)
                result = json.loads((home / "inner-result.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["failure"]["stage"], "copy" if copy_failure else "private-ssh-cleanup")
                self.assertTrue(any(row["stage"] == "private-ssh-cleanup" for row in result["errors"]))

    def test_unreaped_server_timeout_is_recorded(self):
        _, server, _, _ = self.fake_trial()
        server.wait = lambda timeout: (_ for _ in ()).throw(executor.subprocess.TimeoutExpired("server", timeout))
        errors, report = executor.stop_server(server)
        self.assertTrue(errors)
        self.assertFalse(report["reaped"])
        self.assertEqual(server.killed, 1)

    def test_unexpected_private_ssh_exit_is_not_hidden(self):
        _, server, _, _ = self.fake_trial()
        server.code = 1
        errors, report = executor.stop_server(server)
        self.assertTrue(errors)
        self.assertEqual(report["initial_exit_code"], 1)
        self.assertFalse(report["terminate_requested"])

    def test_failed_result_persistence_cannot_return_success(self):
        home, server, fake_run, calls = self.fake_trial()
        original = executor.save_new
        def save(path, value):
            if Path(path).name == "inner-result.json":
                raise PermissionError("cannot persist")
            return original(path, value)
        with patch.object(executor, "HOME", home), patch.object(executor, "read_plan", return_value=self.plan), \
             patch.object(executor, "observed_identity", return_value=self.proof), \
             patch.object(executor, "_run", side_effect=fake_run), \
             patch.object(executor, "save_new", side_effect=save), \
             patch.object(executor.subprocess, "Popen", return_value=server):
            self.assertEqual(executor.inner_main("fake"), 1)
        self.assertEqual(server.waited, 1)

    def test_nonpositive_remaining_deadline_never_spawns_a_child(self):
        with patch.object(executor.time, "monotonic", return_value=10), \
             patch.object(executor.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "deadline expired"):
                executor._run(["not-executed"], self.plan, 120, deadline=9)
            with self.assertRaisesRegex(ValueError, "positive subprocess"):
                executor._run(["not-executed"], self.plan, 0)
        run.assert_not_called()

    def test_protocol_mismatch_fails_before_ssh_or_copy(self):
        with TemporaryDirectory() as folder, patch.object(executor, "HOME", Path(folder)),              patch.object(executor, "read_plan", return_value=self.plan),              patch.object(executor, "observed_identity", return_value=self.proof),              patch.object(executor, "_run", side_effect=[SimpleNamespace(stdout=b"one"),
                                                        SimpleNamespace(stdout=b"two")]) as run,              patch.object(executor.subprocess, "Popen") as start:
            self.assertEqual(executor.inner_main("not-used-by-fake"), 1)
            self.assertEqual(run.call_count, 2)
            start.assert_not_called()


if __name__ == "__main__":
    unittest.main()
