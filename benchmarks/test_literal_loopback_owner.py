"""Offline native-owner state-machine and held-file tests; no service/key/root actions."""
import copy
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmarks import literal_loopback as contract
from benchmarks import literal_loopback_owner as owner

IDENTIFIER = "1" * 32


class Journal:
    def __init__(self, plan):
        self.rows = {"plan": copy.deepcopy(plan)}
    def one(self, name):
        return copy.deepcopy(self.rows.get(name))
    def put(self, name, value):
        if name in self.rows and self.rows[name] != value:
            raise ValueError("journal conflict")
        self.rows[name] = copy.deepcopy(value)


class Port:
    def __init__(self, plan):
        self.plan = plan
        self.starts = self.stops = self.removes = self.closed = 0
        self.started = False
        self.lost_start = self.lost_stop = self.bad_metrics = self.bad_copy = False
        self.bad_stop = False
        self.invocation = "2" * 32
        self.metrics_calls = 0

    def inspect(self):
        return dict(Id=contract.unit_name(IDENTIFIER), LoadState="loaded" if self.started else "not-found",
            ActiveState="active", SubState="exited", Result="success", ExecMainStatus="0",
            Type="exec", RemainAfterExit="yes", InvocationID=self.invocation,
            ControlGroup="/system.slice/" + contract.unit_name(IDENTIFIER),
            Description=self.plan["description"], ExecMainStartTimestampMonotonic="1000000",
            ExecMainExitTimestampMonotonic="2000000")

    def start(self):
        self.starts += 1
        self.started = True
        if self.lost_start:
            raise TimeoutError("lost start reply")

    def capture(self, properties):
        if properties["LoadState"] == "not-found":
            raise ValueError("start remains unknown")
        return contract.RetainedUnit.capture(IDENTIFIER, properties)

    def metrics(self, properties):
        self.metrics_calls += 1
        if self.bad_metrics:
            raise FileNotFoundError("raw memory.peak missing")
        return dict(cpu_total_seconds=1, peak_cgroup_charged_memory_bytes=4096)

    def validate_copy(self):
        if self.bad_copy:
            raise ValueError("tiny content mismatch")
        return dict(fixed_fixture_content_verified=True)

    def stop(self, identity):
        self.stops += 1
        if self.lost_stop:
            raise TimeoutError("lost stop reply")

    def stopped(self, identity):
        if self.bad_stop:
            raise ValueError("owned cgroup remains populated")
        return dict(manager_inactive=True, cgroup_proof="held-original-cgroup-recursively-empty")

    def remove_fixture(self):
        self.removes += 1

    def close(self):
        self.closed += 1


class OwnerTests(unittest.TestCase):
    def setUp(self):
        self.plan = dict(schema=contract.SCHEMA, profile=contract.PROFILE, id=IDENTIFIER,
            unit=contract.unit_name(IDENTIFIER), description="fixed plan")
        self.journal = Journal(self.plan)
        self.port = Port(self.plan)

    def start_intent(self):
        return dict(id=IDENTIFIER,unit=self.plan["unit"],
                    plan_sha256=hashlib.sha256(owner.encoded(self.plan)).hexdigest())

    def run_owner(self):
        return owner.run_owned(self.port, self.journal, self.plan, clock=lambda:1,
                               sleep=lambda _: self.fail("finished fake must not wait"))

    def test_one_start_valid_metrics_and_owned_cleanup(self):
        result = self.run_owner()
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["cleanup"], "verified")
        self.assertEqual((self.port.starts,self.port.stops,self.port.removes,self.port.closed),(1,1,1,1))
        self.assertIn("start-intent",self.journal.rows)
        self.assertIn("unit-identity",self.journal.rows)

    def test_start_intent_precedes_the_only_start_send(self):
        original=self.port.start
        def start():
            self.assertIn("start-intent",self.journal.rows)
            original()
        self.port.start=start
        self.run_owner()
        self.assertEqual(self.port.starts,1)

    def test_lost_start_reply_reconciles_without_resend(self):
        self.port.lost_start=True
        result=self.run_owner()
        self.assertEqual(result["status"],"passed")
        self.assertEqual(self.port.starts,1)
        self.assertIn("start-outcome-uncertain",self.journal.rows)

    def test_existing_intent_without_visible_unit_holds_without_start_or_cleanup(self):
        self.journal.put("start-intent",self.start_intent())
        result=self.run_owner()
        self.assertEqual(result["cleanup"],"held")
        self.assertEqual((self.port.starts,self.port.stops,self.port.removes),(0,0,0))

    def test_existing_intent_observes_existing_unit_without_restart(self):
        self.journal.put("start-intent",self.start_intent())
        self.port.started=True
        self.run_owner()
        self.assertEqual(self.port.starts,0)

    def test_replacement_invocation_is_not_stopped_or_cleaned(self):
        self.journal.put("start-intent",self.start_intent())
        self.journal.put("unit-identity",dict(unit=self.plan["unit"],invocation_id="3"*32,
            cgroup="/system.slice/"+self.plan["unit"]))
        self.port.started=True
        result=self.run_owner()
        self.assertEqual(result["cleanup"],"held")
        self.assertEqual((self.port.stops,self.port.removes),(0,0))

    def test_missing_raw_metrics_fail_acceptance_even_if_cleanup_succeeds(self):
        self.port.bad_metrics=True
        result=self.run_owner()
        self.assertEqual(result["status"],"failed")
        self.assertIsNone(result["metrics"])
        self.assertEqual(result["cleanup"],"verified")
        self.assertEqual(self.port.removes,1)

    def test_content_mismatch_fails_and_still_retires_owned_unit(self):
        self.port.bad_copy=True
        result=self.run_owner()
        self.assertEqual(result["status"],"failed")
        self.assertEqual(result["cleanup"],"verified")

    def test_unproven_stop_preserves_fixture_and_records_held_result(self):
        self.port.bad_stop=True
        result=self.run_owner()
        self.assertEqual(result["status"],"failed")
        self.assertEqual(result["cleanup"],"held")
        self.assertEqual(self.port.removes,0)
        self.assertIn("held",self.journal.rows)
        self.assertNotIn("result",self.journal.rows)

    def test_lost_stop_reply_uses_positive_readback_without_repeat(self):
        self.port.lost_stop=True
        result=self.run_owner()
        self.assertEqual(result["cleanup"],"verified")
        self.assertEqual(self.port.stops,1)

    def test_prior_uncertain_stop_is_not_resent(self):
        self.journal.put("start-intent",self.start_intent())
        self.port.started=True
        self.journal.put("stop-intent",dict(unit=self.plan["unit"],invocation_id="2"*32))
        self.port.bad_stop=True
        result=self.run_owner()
        self.assertEqual(result["cleanup"],"held")
        self.assertEqual(self.port.stops,0)

    def test_completed_result_is_replayed_without_actions(self):
        first=self.run_owner()
        second=self.run_owner()
        self.assertEqual(first,second)
        self.assertEqual((self.port.starts,self.port.stops,self.port.removes),(1,1,1))

    def test_changed_plan_is_rejected_before_any_actions(self):
        self.journal.rows["plan"]["description"]="other"
        with self.assertRaisesRegex(ValueError,"retained plan"):
            self.run_owner()
        self.assertEqual(self.port.starts,0)

    def test_retained_result_persistence_failure_still_closes_held_fd(self):
        original=self.journal.put
        def put(name,value):
            if name=="result":raise OSError("full evidence filesystem")
            original(name,value)
        self.journal.put=put
        with self.assertRaises(OSError):self.run_owner()
        self.assertEqual(self.port.closed,1)
        self.assertEqual(self.port.removes,1)


class HeldInputTests(unittest.TestCase):
    def test_copied_elf_is_the_hashed_held_input_with_fresh_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            raw=bytearray(128)
            raw[:6]=b"\x7fELF\x02\x01"
            raw[18:20]=(62).to_bytes(2,"little")
            source=root/"source";source.write_bytes(raw);source.chmod(0o6755)
            destination=root/"dest";destination.mkdir()
            fd=os.open(destination,os.O_RDONLY|os.O_DIRECTORY)
            try:
                owner.copy_reviewed_elf(str(source),fd,"rcp",hashlib.sha256(raw).hexdigest())
            finally:os.close(fd)
            self.assertEqual((destination/"rcp").read_bytes(),raw)
            self.assertEqual((destination/"rcp").stat().st_mode&0o7777,0o755)

    def test_symlink_source_and_wrong_digest_are_rejected_without_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);source=root/"source";source.write_bytes(b"x"*128)
            link=root/"link";link.symlink_to(source)
            fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
            try:
                for path in (link,source):
                    with self.subTest(path=path),self.assertRaises((OSError,ValueError)):
                        owner.copy_reviewed_elf(str(path),fd,"output","0"*64)
                self.assertFalse((root/"output").exists())
            finally:os.close(fd)

    def test_preexisting_output_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            fd=os.open(temp,os.O_RDONLY|os.O_DIRECTORY)
            try:
                owner.write_new(fd,"record",b"first")
                with self.assertRaises(FileExistsError):owner.write_new(fd,"record",b"second")
                self.assertEqual(owner.held_read(fd,"record",100),b"first")
            finally:os.close(fd)

    def test_regular_filesystem_cannot_supply_cgroup_unlink_proof(self):
        with tempfile.TemporaryDirectory() as temp:
            fd=os.open(temp,os.O_RDONLY|os.O_DIRECTORY)
            try:
                with self.assertRaisesRegex(ValueError,"cgroup v2"):
                    owner.require_cgroup2(fd)
            finally:os.close(fd)

    def test_counter_paths_cannot_escape_held_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            fd=os.open(temp,os.O_RDONLY|os.O_DIRECTORY)
            try:
                for name in ("../outside","a/b","/etc/passwd"):
                    with self.subTest(name=name),self.assertRaises(ValueError):
                        owner.held_read(fd,name,100)
            finally:os.close(fd)

    def test_private_wrapper_pins_strict_host_checking_before_current_rcp_option(self):
        import shutil
        import subprocess
        executable=shutil.which("ssh")
        self.assertIsNotNone(executable,"OpenSSH config parser required by benchmark tests")
        with tempfile.TemporaryDirectory() as temp:
            config=Path(temp)/"config"
            config.write_text(contract.ssh_client_config("runner"))
            # -G evaluates only this controlled config; no Match/Include, key read or network.
            result=subprocess.run([executable,"-G","-F",str(config),
                "-o","StrictHostKeyChecking=yes","-o","StrictHostKeyChecking=no",
                "127.0.0.1"],stdin=subprocess.DEVNULL,capture_output=True,text=True,check=True)
            values=dict(line.split(" ",1) for line in result.stdout.splitlines() if " " in line)
            self.assertEqual(values["stricthostkeychecking"],"true")
            self.assertEqual(values["hostname"],"127.0.0.1")
            self.assertEqual(values["port"],"2222")
            self.assertEqual(values["identityagent"],"none")
            self.assertEqual(values["userknownhostsfile"],contract.GUEST+"/home/known_hosts")
        wrapper=contract.ssh_wrapper()
        self.assertIn('exec /usr/bin/ssh -F '+contract.GUEST+'/ssh_config -o StrictHostKeyChecking=yes',wrapper)
        self.assertTrue(wrapper.endswith(' \"$@\"\n'))

    def test_root_entry_requires_explicit_privilege_before_any_preparation(self):
        with patch.object(owner.os,"geteuid",return_value=1001),patch.object(owner,"prepare") as prepare:
            with self.assertRaisesRegex(ValueError,"explicit root"):
                owner.main(["--run-reviewed-native-smoke"])
            prepare.assert_not_called()




class NativeContractTests(unittest.TestCase):
    def setUp(self):
        self.plan=dict(id=IDENTIFIER,account="runner",uid=1001,gid=1001,
                       description="fixed plan",unit=contract.unit_name(IDENTIFIER))
        self.properties=Port(self.plan).inspect()
        self.properties["LoadState"]="loaded"

    def test_start_request_is_one_bounded_private_unit_not_wait_for_retained_exit(self):
        port=owner.NativePort(self.plan)
        with patch.object(owner,"command") as command:
            port.start()
        command.assert_called_once()
        argv=command.call_args.args[0]
        self.assertEqual(argv[0],"/usr/bin/systemd-run")
        self.assertNotIn("--wait",argv)
        self.assertIn("--property=User=1001",argv)
        self.assertIn("--property=PrivateNetwork=yes",argv)
        self.assertIn("--property=MemoryMax=512M",argv)
        self.assertEqual(argv[-2:],["_inner",contract.GUEST+"/plan.json"])

    def test_changed_invocation_blocks_the_name_bound_stop_before_send(self):
        port=owner.NativePort(self.plan)
        identity=contract.RetainedUnit.capture(IDENTIFIER,self.properties)
        changed=dict(self.properties,InvocationID="3"*32)
        with patch.object(port,"inspect",return_value=changed),patch.object(owner,"command") as command:
            with self.assertRaisesRegex(ValueError,"identity changed"):port.stop(identity)
        command.assert_not_called()

    def test_missing_cgroup_capture_keeps_manager_identity_for_stop_but_holds_deletion(self):
        port=owner.NativePort(self.plan)
        properties=dict(self.properties)
        with patch.object(owner,"root_directory",side_effect=FileNotFoundError("no cgroup")):
            identity=port.capture(properties)
        self.assertIsNone(port.cg)
        self.assertEqual(identity.invocation_id,"2"*32)
        with patch.object(port,"inspect",return_value=properties),patch.object(owner,"command") as command:
            port.stop(identity)
        command.assert_called_once()
        stopped=dict(properties,ActiveState="inactive",SubState="dead",ControlGroup="")
        with patch.object(port,"inspect",return_value=stopped):
            with self.assertRaisesRegex(ValueError,"preserve fixture"):port.stopped(identity)

    def test_current_path_counter_files_are_not_a_fallback_for_missing_held_fd(self):
        port=owner.NativePort(self.plan)
        port.identity=contract.RetainedUnit.capture(IDENTIFIER,self.properties)
        with self.assertRaisesRegex(ValueError,"held cgroup identity missing"):
            port.metrics(self.properties)

    def test_duplicate_unknown_and_malformed_manager_properties_are_rejected(self):
        for raw in (b"Id=a\nId=b\n",b"Other=unknown\n",b"Id-without-equals\n"):
            with self.subTest(raw=raw),self.assertRaises(ValueError):
                owner.parse_properties(raw)


class PreparationTests(unittest.TestCase):
    def preparation(self, fail_second_key=False):
        from types import SimpleNamespace
        import pwd
        record=SimpleNamespace(pw_name="runner",pw_uid=1001,pw_gid=1001)
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root=Path(temp.name)
        fixtures,evidence=root/"fixtures",root/"evidence"
        def directories(path,create=False):
            Path(path).mkdir(mode=0o700,parents=True,exist_ok=True)
            return os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        raw=bytearray(128);raw[:6]=b"\x7fELF\x02\x01";raw[18:20]=(62).to_bytes(2,"little")
        binary=root/"reviewed-binary";binary.write_bytes(raw)
        digest=hashlib.sha256(raw).hexdigest()
        args=SimpleNamespace(account=record.pw_name,source_commit="a"*40,ack_reviewed_binaries=True,
            rcp=str(binary),rcp_sha256=digest,rcpd=str(binary),rcpd_sha256=digest)
        tools=[]
        def tool(argv,**kwargs):
            tools.append(argv)
            self.assertEqual(argv[0],"/usr/bin/ssh-keygen")
            self.assertEqual(argv[argv.index("-N")+1],"")
            path=Path(argv[argv.index("-f")+1])
            if fail_second_key and path.name=="identity":raise OSError("synthetic key-tool failure")
            path.write_bytes(b"not-a-real-private-key")
            path.chmod(0o600)
            Path(str(path)+".pub").write_text("ssh-ed25519 AAAA rcp-loopback-fixture\n")
            return SimpleNamespace(returncode=0,stdout=b"",stderr=b"")
        class PreparedJournal(Journal):
            def __init__(self,path):
                self.rows={}
            def close(self):pass
        original_read_text=Path.read_text
        def read_text(path,*args,**kwargs):
            if str(path)=="/etc/passwd":
                return "root:x:0:0:root:/root:/bin/sh\nrunner:x:1001:1001:fixture:/nonexistent:/bin/false\n"
            return original_read_text(path,*args,**kwargs)
        with patch.object(owner,"require_native_host"),              patch.object(owner.pwd,"getpwnam",return_value=record),              patch.object(owner.os,"chown"),patch.object(owner.os,"fchown"), \
             patch.object(owner.os,"readlink",return_value="net:[offline-fixture]"),              patch.object(Path,"read_text",read_text),              patch.object(owner,"FIXTURES",fixtures),patch.object(owner,"EVIDENCE",evidence), \
             patch.object(owner,"root_directory",side_effect=directories), \
             patch.object(owner,"Journal",PreparedJournal),patch.object(owner,"command",side_effect=tool), \
             patch.object(owner.os,"geteuid",return_value=0), \
             patch.object(owner.os,"getgrouplist",return_value=[record.pw_gid]):
            if fail_second_key:
                with self.assertRaisesRegex(OSError,"synthetic"):owner.prepare(args)
                self.assertEqual(list(fixtures.iterdir()),[])
                return
            plan,journal=owner.prepare(args)
        fixture=fixtures/plan["id"]
        self.assertEqual(fixture.stat().st_mode&0o777,0o755)
        self.assertEqual((fixture/"bin").stat().st_mode&0o777,0o755)
        self.assertEqual((fixture/"plan.json").stat().st_mode&0o777,0o644)
        self.assertEqual((fixture/"home").stat().st_mode&0o777,0o700)
        self.assertEqual((fixture/"bin/rcp").read_bytes(),raw)
        self.assertEqual(journal.one("plan"),plan)
        self.assertNotIn(b"not-a-real-private-key",owner.encoded(plan))
        self.assertEqual(len(tools),2)
        self.assertIn("[127.0.0.1]:2222 ssh-ed25519 AAAA",
                      (fixture/"home/known_hosts").read_text())
        self.assertEqual((fixture/"home/.ssh/config").read_bytes(),(fixture/"ssh_config").read_bytes())

    def test_real_fixture_files_and_modes_with_only_fake_key_tool(self):
        previous=os.umask(0o077)
        try:self.preparation()
        finally:os.umask(previous)

    def test_preparation_failure_removes_only_the_new_fixture_without_service_start(self):
        self.preparation(fail_second_key=True)


if __name__=="__main__":
    unittest.main()
