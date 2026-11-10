"""Offline literal-loopback contract checks; no services, keys or network calls."""
import copy
import unittest

from benchmarks import literal_loopback as loop

ID = "1" * 32


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.properties = dict(Id=loop.unit_name(ID), InvocationID="2" * 32,
            ControlGroup="/system.slice/" + loop.unit_name(ID), Type="exec",
            RemainAfterExit="yes", ActiveState="active", SubState="exited",
            Result="success", ExecMainStatus="0",
            ExecMainStartTimestampMonotonic="1000000",
            ExecMainExitTimestampMonotonic="3000000")
        self.events = "populated 0\nfrozen 0\n"
        self.cpu = "usage_usec 3500000\nuser_usec 2000000\nsystem_usec 1500000\n"
        self.peak = "67108864\n"

    def metrics(self):
        return loop.RetainedUnit.capture(ID, self.properties).final_metrics(
            self.properties, events=self.events, cpu_stat=self.cpu, memory_peak=self.peak)

    def test_private_account_view_refuses_to_copy_password_hashes(self):
        with self.assertRaisesRegex(ValueError,"credential material"):
            loop.account_view("root:$6$synthetic$not-a-real-hash:0:0:root:/root:/bin/sh\n"
                "runner:x:1001:1001:fixture:/home/runner:/bin/sh\n","runner",1001,1001)

    def test_unit_identity_is_not_an_arbitrary_control_target(self):
        for value in ("", "../other", "a" * 33, ID.upper().replace("1", "A"), "2.service"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                loop.unit_name(value)

    def test_native_plan_preserves_nonroot_private_network_and_hides_host_sockets(self):
        unit, properties = loop.service_properties(identifier=ID, account="runner", uid=1001,
            gid=1001, host_root="/scratch/literal/trials/" + ID)
        self.assertEqual(unit, loop.unit_name(ID))
        self.assertEqual(properties["User"], "1001")
        self.assertEqual(properties["Type"], "exec")
        self.assertEqual(properties["RemainAfterExit"], "yes")
        self.assertEqual(properties["RestrictAddressFamilies"], "AF_UNIX AF_INET")
        for name in ("PrivateNetwork", "NoNewPrivileges", "ProtectControlGroups",
                     "RestrictNamespaces"):
            self.assertEqual(properties[name], "yes")
        self.assertEqual(properties["CapabilityBoundingSet"], "")
        self.assertIn("/run:ro", properties["TemporaryFileSystem"])
        self.assertIn("-/mnt", properties["InaccessiblePaths"])
        self.assertIn("/passwd:/etc/passwd", properties["BindReadOnlyPaths"])
        self.assertEqual(properties["ReadWritePaths"],
                         loop.GUEST + "/home " + loop.GUEST + "/destination")

    def test_root_identity_and_unsafe_fixture_paths_are_rejected(self):
        for edit in (dict(uid=0), dict(gid=0), dict(account="root"), dict(account="root\nUser=other"),
                     dict(host_root="/"), dict(host_root="//scratch/literal/trials/" + ID),
                     dict(host_root="/scratch/../literal/" + ID),
                     dict(host_root="/scratch/%h/trial/" + ID),
                     dict(host_root="/scratch/literal/trials/other"),
                     dict(host_root="/scratch/a b/trials/" + ID)):
            with self.subTest(edit=edit):
                args = dict(identifier=ID, account="runner", uid=1001, gid=1001,
                            host_root="/scratch/literal/trials/" + ID)
                args.update(edit)
                with self.assertRaises(ValueError):
                    loop.service_properties(**args)

    def test_private_account_view_changes_only_home_and_shell(self):
        text = "root:x:0:0:root:/root:/bin/bash\nrunner:x:1001:1001:Runner:/nonexistent:/usr/sbin/nologin\n"
        value = loop.account_view(text, "runner", 1001, 1001)
        self.assertEqual(value.splitlines()[0], text.splitlines()[0])
        self.assertEqual(value.splitlines()[1],
            "runner:x:1001:1001:Runner:" + loop.GUEST + "/home:/bin/sh")
        for bad in (text + text.splitlines()[1] + "\n", text.replace("1001:1001", "1002:1001"),
                    text + "alias:x:1001:1001:Alias:/other:/bin/sh\n",
                    text + "invalid:row\n"):
            with self.assertRaises(ValueError):
                loop.account_view(bad, "runner", 1001, 1001)

    def test_both_operands_and_daemon_bind_use_literal_loopback_with_tls_default(self):
        argv = loop.copy_argv()
        self.assertIn("--bind-ip=127.0.0.1", argv)
        self.assertIn("--force-remote", argv)
        self.assertTrue(all(x.startswith("127.0.0.1:") for x in argv[-2:]))
        self.assertNotIn("--no-encryption", argv)
        self.assertFalse(any("192.0.2." in x or "localhost" in x for x in argv))

    def test_private_ssh_configuration_has_no_host_listener_or_trust_fallback(self):
        server = loop.ssh_server_config("runner")
        client = loop.ssh_client_config("runner")
        self.assertEqual([x for x in server.splitlines() if x.startswith("ListenAddress")],
                         ["ListenAddress 127.0.0.1"])
        for line in ("PasswordAuthentication no", "PermitRootLogin no",
                     "DisableForwarding yes", "PermitUserEnvironment no"):
            self.assertIn(line, server)
        for line in ("IdentityAgent none", "StrictHostKeyChecking yes", "ProxyCommand none",
                     "ProxyJump none", "GlobalKnownHostsFile /dev/null"):
            self.assertIn(line, client)

    def test_endpoint_inclusive_cpu_and_distinct_memory_scope(self):
        result = self.metrics()
        self.assertEqual(result["cpu_total_seconds"], 3.5)
        self.assertEqual(result["unit_duration_seconds"], 2)
        self.assertEqual(result["peak_cgroup_charged_memory_bytes"], 64 * 1024 * 1024)
        self.assertIn("not RSS", result["memory_scope"])
        self.assertIn("setup and teardown", result["wall_scope"])
        self.assertEqual(result["profile"], loop.PROFILE)

    def test_retained_identity_must_match_manager_and_exact_private_cgroup(self):
        for key, value in (("Id", "other.service"), ("InvocationID", "0" * 32),
                           ("ControlGroup", "/"), ("ControlGroup", "/system.slice/other.service"),
                           ("Type", "oneshot"), ("RemainAfterExit", "no")):
            with self.subTest(key=key, value=value):
                saved = self.properties[key]
                self.properties[key] = value
                with self.assertRaises(ValueError):
                    self.metrics()
                self.properties[key] = saved

    def test_restarted_unit_cannot_supply_final_counters(self):
        held = loop.RetainedUnit.capture(ID, self.properties)
        final = copy.deepcopy(self.properties)
        final["InvocationID"] = "3" * 32
        with self.assertRaisesRegex(ValueError, "identity changed"):
            held.final_metrics(final, events=self.events, cpu_stat=self.cpu, memory_peak=self.peak)

    def test_live_failed_or_stopped_units_are_not_success(self):
        for key, value in (("ActiveState", "inactive"), ("SubState", "running"),
                           ("Result", "timeout"), ("ExecMainStatus", "1")):
            with self.subTest(key=key):
                saved = self.properties[key]
                self.properties[key] = value
                with self.assertRaisesRegex(ValueError, "finish successfully"):
                    self.metrics()
                self.properties[key] = saved

    def test_recursive_residual_processes_block_final_read(self):
        self.events = "populated 1\nfrozen 0\n"
        with self.assertRaisesRegex(ValueError, "descendants"):
            self.metrics()

    def test_absent_invalid_duplicate_or_zero_counters_never_fall_back(self):
        for field, value in (("cpu", ""), ("cpu", "user_usec 1\nuser_usec 2\n"),
                             ("cpu", "user_usec -1\nsystem_usec 2\n"),
                             ("peak", "max\n"), ("peak", "0\n"),
                             ("events", "frozen 0\n")):
            with self.subTest(field=field, value=value):
                saved = getattr(self, field)
                setattr(self, field, value)
                with self.assertRaises(ValueError):
                    self.metrics()
                setattr(self, field, saved)

    def test_timestamp_errors_and_overlong_unit_fail(self):
        for start, end in (("0", "3000000"), ("3000000", "2000000"),
                           ("1", "200000001"), ("missing", "3000000")):
            with self.subTest(start=start, end=end):
                self.properties["ExecMainStartTimestampMonotonic"] = start
                self.properties["ExecMainExitTimestampMonotonic"] = end
                with self.assertRaises(ValueError):
                    self.metrics()


if __name__ == "__main__":
    unittest.main()
