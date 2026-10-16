"""Offline failure-path tests; no credentials, AWS calls, formatting, or launches."""

import copy
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import tempfile
import struct
import unittest
from unittest.mock import Mock, patch
import zipfile

from infra.aws_benchmark import common, controller, operator, worker


NOW = datetime(2026, 10, 16, tzinfo=timezone.utc)


def window(now=NOW):
    return dict(run="123-1", baseline="a" * 40, candidate="a" * 40, suite="aa-smoke",
                opens=common.timestamp(now + timedelta(minutes=3)),
                deadline=common.timestamp(now + timedelta(minutes=20)))


def closed(maximum=0):
    return dict(MinSize=0, DesiredCapacity=0, MaxSize=maximum, Instances=[],
                SuspendedProcesses=[dict(ProcessName=p) for p in common.SUSPEND],
                LaunchTemplate=dict(LaunchTemplateId="lt-test", Version="1"))


class Missing(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeCloud:
    def __init__(self):
        self.events, self.actions, self.objects = [], [], {}
        self.fail = None
        self.api = {name: Mock() for name in ("s3", "autoscaling", "ec2")}
        asg, ec2, s3 = (self.api[k] for k in ("autoscaling", "ec2", "s3"))
        asg.describe_auto_scaling_groups.return_value = {"AutoScalingGroups": [closed()]}
        asg.describe_scheduled_actions.side_effect = lambda **kw: {"ScheduledUpdateGroupActions": copy.deepcopy(self.actions)}
        asg.describe_policies.return_value = {"ScalingPolicies": []}
        asg.describe_lifecycle_hooks.return_value = {"LifecycleHooks": []}
        asg.describe_warm_pool.return_value = {}
        asg.describe_instance_refreshes.return_value = {"InstanceRefreshes": []}
        ec2.describe_instances.return_value = {"Reservations": []}
        ec2.describe_launch_template_versions.return_value = {"LaunchTemplateVersions": [
            {"LaunchTemplateData": {"InstanceType": "c5d.24xlarge", "ImageId": "ami-123"}}]}
        asg.put_scheduled_update_group_action.side_effect = self.schedule
        s3.get_object.side_effect = self.get
        s3.put_object.side_effect = self.put

    def schedule(self, **kwargs):
        self.events.append(kwargs["ScheduledActionName"])
        if self.fail == self.events[-1]:
            raise RuntimeError("injected schedule failure")
        self.actions.append(kwargs)

    def put(self, Bucket, Key, Body, **kwargs):
        self.events.append(Key)
        if self.fail == Key:
            raise RuntimeError("injected upload failure")
        self.objects[Key] = Body

    def get(self, Bucket, Key):
        if Key not in self.objects:
            raise Missing()
        data = self.objects[Key]
        return {"ContentLength": len(data), "Body": io.BytesIO(data)}


class WindowTests(unittest.TestCase):
    def test_close_is_persisted_before_control_and_open(self):
        cloud = FakeCloud()
        operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
        self.assertEqual(cloud.events, ["close-123-1", "control/window.json", "open-123-1"])
        self.assertEqual([a["MaxSize"] for a in cloud.actions], [0, 1])
        cloud.api["autoscaling"].update_auto_scaling_group.assert_not_called()
        cloud.api["autoscaling"].set_desired_capacity.assert_not_called()

    def test_each_arming_failure_leaves_no_launch_and_never_removes_cutoff(self):
        for failure in ("close-123-1", "control/window.json", "open-123-1"):
            with self.subTest(failure=failure):
                cloud = FakeCloud()
                cloud.fail = failure
                with self.assertRaises(RuntimeError):
                    operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
                self.assertFalse(any(a["MaxSize"] == 1 for a in cloud.actions))
                cloud.api["autoscaling"].delete_scheduled_action.assert_not_called()

    def test_unverified_shutdown_never_opens(self):
        cloud = FakeCloud()
        cloud.api["autoscaling"].describe_scheduled_actions.return_value = {"ScheduledUpdateGroupActions": []}
        cloud.api["autoscaling"].describe_scheduled_actions.side_effect = None
        with self.assertRaisesRegex(ValueError, "shutdown is missing"):
            operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
        self.assertEqual(cloud.events, ["close-123-1"])

    def test_active_or_old_pending_window_is_not_overwritten(self):
        for kind in ("capacity", "instance", "schedule", "control"):
            with self.subTest(kind=kind):
                cloud = FakeCloud()
                if kind == "capacity":
                    cloud.api["autoscaling"].describe_auto_scaling_groups.return_value["AutoScalingGroups"][0]["MaxSize"] = 1
                elif kind == "instance":
                    cloud.api["ec2"].describe_instances.return_value = {"Reservations": [{"Instances": [{}]}]}
                elif kind == "schedule":
                    cloud.actions = [{"ScheduledActionName": "old-open"}]
                else:
                    cloud.objects["control/window.json"] = json.dumps(window()).encode()
                with self.assertRaises(ValueError):
                    operator.arm(cloud.api, "group", "bucket", window(), now=lambda: NOW)
                self.assertEqual(cloud.events, [])

    def test_expired_window_and_invalid_revision_are_rejected_before_aws(self):
        for changes in ({"deadline": common.timestamp(NOW)}, {"baseline": "main"},
                        {"candidate": "b" * 40}, {"suite": "shell"}, {"run": "../123"}):
            cloud = FakeCloud()
            with self.assertRaises(ValueError):
                operator.arm(cloud.api, "group", "bucket", {**window(), **changes}, now=lambda: NOW)
            self.assertEqual(cloud.events, [])

    def test_opening_lead_time_cannot_be_consumed_by_upload(self):
        cloud = FakeCloud()
        clock = iter([NOW, NOW, NOW + timedelta(minutes=3)])
        with self.assertRaisesRegex(ValueError, "lead time"):
            operator.arm(cloud.api, "group", "bucket", window(), now=lambda: next(clock))
        self.assertEqual(cloud.events, ["close-123-1", "control/window.json"])

    def test_dangerous_drift_is_rejected(self):
        for changes in ({"SuspendedProcesses": []}, {"MaxInstanceLifetime": 86400},
                        {"NewInstancesProtectedFromScaleIn": True}, {"CapacityRebalance": True},
                        {"InstanceLifecyclePolicy": {"RetentionTriggers": {"TerminateHookAbandon": "retain"}}},
                        {"TerminationPolicies": ["arn:aws:lambda:custom"]},
                        {"LaunchTemplate": {"Version": "$Latest"}}):
            with self.assertRaises(ValueError):
                common.verify_group({**closed(), **changes}, 0)


class ControllerTests(unittest.TestCase):
    def test_cleanup_retries_uncertain_zero_request(self):
        cloud = FakeCloud()
        cloud.api["autoscaling"].set_desired_capacity.side_effect = [TimeoutError(), {}]
        with patch.object(controller.time, "sleep"):
            controller.cleanup(cloud.api, "group")
        self.assertEqual(cloud.api["autoscaling"].set_desired_capacity.call_count, 2)

    def test_uncertain_launch_response_still_requests_teardown(self):
        cloud = FakeCloud()
        now = datetime.now(timezone.utc)
        lease = window(now - timedelta(minutes=4))
        cloud.objects["control/window.json"] = json.dumps(lease).encode()
        cloud.actions = [dict(ScheduledActionName="close-123-1", StartTime=lease["deadline"],
                              MinSize=0, DesiredCapacity=0, MaxSize=0)]
        cloud.api["autoscaling"].describe_auto_scaling_groups.return_value = {"AutoScalingGroups": [closed(1)]}
        cloud.api["autoscaling"].set_desired_capacity.side_effect = [TimeoutError("uncertain"), {}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for key in common.BINARIES:
                (root / key).parent.mkdir(exist_ok=True)
                (root / key).write_bytes(b"binary")
            with self.assertRaisesRegex(TimeoutError, "uncertain"):
                controller.run(cloud.api, "group", "bucket", {k: lease[k] for k in ("run", "baseline", "candidate", "suite")}, root, root / "out")
        calls = cloud.api["autoscaling"].set_desired_capacity.call_args_list
        self.assertEqual([c.kwargs["DesiredCapacity"] for c in calls], [1, 0])

    def test_teardown_waits_for_physical_instance_not_just_empty_group(self):
        cloud = FakeCloud()
        cloud.api["ec2"].describe_instances.side_effect = [
            {"Reservations": [{"Instances": [{"State": {"Name": "shutting-down"}}]}]}, {"Reservations": []}]
        with patch.object(controller.time, "sleep") as sleep:
            controller.cleanup(cloud.api, "group")
        sleep.assert_called_once_with(10)

    def test_teardown_timeout_is_failure(self):
        cloud = FakeCloud()
        with self.assertRaisesRegex(TimeoutError, "unconfirmed"):
            controller.cleanup(cloud.api, "group", timeout=0)

    def test_budget_cannot_be_extended_by_backward_wall_clock(self):
        with patch.object(common.time, "time", return_value=1000), patch.object(common.time, "monotonic", return_value=10):
            budget = common.Budget(datetime.fromtimestamp(1100, timezone.utc))
        with patch.object(common.time, "time", return_value=900), patch.object(common.time, "monotonic", return_value=111):
            with self.assertRaises(TimeoutError):
                budget.left()


class WorkerTests(unittest.TestCase):
    def test_cache_helper_only_exposes_fixed_operation_to_worker_uid(self):
        for uid, request, valid in ((123, b"drop\n", True), (0, b"drop\n", False), (123, b"shell\n", False)):
            connection = Mock()
            connection.getsockopt.return_value = struct.pack("3i", 1, uid, 123)
            connection.recv.return_value = request
            with patch.object(worker.os, "sync") as sync, patch.object(worker.Path, "write_text") as write:
                if valid:
                    worker.handle_cache(connection, Mock(), 123)
                    sync.assert_called_once()
                    write.assert_called_once_with("3\n")
                    connection.sendall.assert_called_once_with(b"ok\n")
                else:
                    with self.assertRaises(ValueError):
                        worker.handle_cache(connection, Mock(), 123)
                    sync.assert_not_called()
                    write.assert_not_called()

    def test_worker_failures_never_become_success_or_extend_deadline(self):
        for phase in ("digest", "format", "measure", "stop", "collect", "upload"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory, ExitStack() as patches:
                cloud = FakeCloud()
                lease = window(datetime.now(timezone.utc))
                request = {k: lease[k] for k in ("run", "baseline", "candidate", "suite", "deadline")}
                request["sha256"] = {key: common.digest(b"binary") for key in common.BINARIES}
                cloud.objects.update({"input/" + key: b"binary" for key in common.BINARIES})
                if phase == "digest":
                    request["sha256"]["candidate/rcp"] = "0" * 64
                cloud.objects["input/request.json"] = json.dumps(request).encode()
                cloud.objects["control/window.json"] = json.dumps(lease).encode()
                root = Path(directory)
                (root / "revision.txt").write_text("trusted-harness")
                for attr, value in (("ROOT", root), ("STATE", root / "state"), ("DATA", root / "data")):
                    patches.enter_context(patch.object(worker, attr, value))
                patches.enter_context(patch.object(worker.pwd, "getpwnam", return_value=Mock(pw_uid=123, pw_gid=123)))
                patches.enter_context(patch.object(worker.signal, "signal"))
                timer = patches.enter_context(patch.object(worker.signal, "setitimer"))
                prepare = patches.enter_context(patch.object(worker, "prepare_storage", return_value=["disk1", "disk2"]))
                patches.enter_context(patch.object(worker, "cache_server"))
                measure = patches.enter_context(patch.object(worker.subprocess, "run", return_value=Mock(returncode=0)))
                stop = patches.enter_context(patch.object(worker, "stop_unit"))
                collect = patches.enter_context(patch.object(worker, "collect", return_value=b"zip"))
                if phase in ("format", "measure", "stop", "collect"):
                    {"format": prepare, "measure": measure, "stop": stop, "collect": collect}[phase].side_effect = TimeoutError()
                if phase == "upload":
                    cloud.fail = "output/results.zip"
                worker.run(cloud.api["s3"], "bucket")
                status = json.loads(cloud.objects["output/status.json"])
                self.assertFalse(status["success"])
                self.assertEqual(timer.call_count, 1)
                if phase in ("digest", "format"):
                    measure.assert_not_called()
                if phase == "measure":
                    stop.assert_called_once()
                if phase == "stop":
                    collect.assert_not_called()

    def test_successfully_unloaded_unit_is_safe_to_collect(self):
        with patch.object(worker.subprocess, "run") as stop, \
             patch.object(worker.subprocess, "check_output", return_value="inactive\n"), \
             patch.object(worker.Path, "exists", return_value=False):
            stop.return_value.returncode = 5
            worker.stop_unit()

    def test_remaining_candidate_processes_prevent_collection(self):
        with patch.object(worker.subprocess, "run"), \
             patch.object(worker.subprocess, "check_output", return_value="inactive\n"), \
             patch.object(worker.Path, "exists", return_value=True), \
             patch.object(worker.Path, "read_text", return_value="populated 1\nfrozen 0\n"):
            with self.assertRaisesRegex(ValueError, "still contains"):
                worker.stop_unit()

    def test_only_unused_instance_store_disks_are_selected(self):
        disks = [dict(path=f"/dev/nvme{i}n1", type="disk", size=900_000_000_000,
                      model="Amazon EC2 NVMe Instance Storage", mountpoints=[None], fstype=None, ro=False)
                 for i in range(1, 5)]
        root = dict(path="/dev/nvme0n1", model="Amazon Elastic Block Store", children=[{"mountpoints": ["/"]}])
        self.assertEqual(worker.choose_disks({"blockdevices": [root, *disks]}), ["/dev/nvme1n1", "/dev/nvme2n1"])
        for changes in ({"mountpoints": ["/"]}, {"children": [{}]}, {"fstype": "ext4"},
                        {"path": "/dev/sda"}, {"size": 1}, {"ro": True}):
            broken = copy.deepcopy(disks)
            broken[0].update(changes)
            with self.assertRaises(ValueError):
                worker.choose_disks({"blockdevices": broken})

    def test_result_collector_rejects_links_fifos_and_oversize_files(self):
        for kind in ("symlink", "hardlink", "fifo", "size"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "file"
                if kind == "symlink":
                    source.symlink_to("/etc/passwd")
                elif kind == "hardlink":
                    source.write_bytes(b"x")
                    os.link(source, root / "link")
                elif kind == "fifo":
                    os.mkfifo(source)
                else:
                    source.write_bytes(b"x" * 100)
                with patch.object(worker, "MAX_RESULT", 65550), self.assertRaises((OSError, ValueError)):
                    worker.collect(root, {}, Mock())

    def test_partial_results_are_preserved_without_interpreting_content(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "partial.json").write_text('{"status":"running"}')
            result = worker.collect(root, {"success": False}, Mock())
        with zipfile.ZipFile(io.BytesIO(result)) as archive:
            self.assertEqual(archive.read("benchmark/partial.json"), b'{"status":"running"}')
            self.assertFalse(json.loads(archive.read("worker.json"))["success"])

    def test_complete_trial_tree_is_collected(self):
        # run.py puts each command's logs/resources under case/variant/iteration.
        # Include the worker's outer results/ directory, both paired roles and repetitions.
        files = {
            "results/results.json": b'{"status":"completed"}',
            "results/summary.md": b"A/A smoke\n",
            "results/logs/tiny-10k.filegen.stdout.log": b"generated\n",
            "results/logs/tiny-10k.filegen.stderr.log": b"",
        }
        for variant in ("rcp-baseline", "rcp-default"):
            for iteration in (1, 2):
                trial = f"results/logs/tiny-10k/{variant}/{iteration}"
                files.update({f"{trial}/0.stdout.log": b"copied\n",
                              f"{trial}/0.stderr.log": b"",
                              f"{trial}/resources.json": b'{"status":"complete"}'})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, content in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            result = worker.collect(root, {"success": True}, Mock())
        with zipfile.ZipFile(io.BytesIO(result)) as archive:
            self.assertEqual(set(archive.namelist()), {"worker.json", *("benchmark/" + name for name in files)})
            for name, content in files.items():
                self.assertEqual(archive.read("benchmark/" + name), content)

    def test_result_collector_still_rejects_excessive_nesting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results/logs/tiny-10k/rcp-default/1/extra").mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "nesting limit"):
                worker.collect(root, {}, Mock())

    def test_untrusted_metadata_name_cannot_duplicate_trusted_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "worker.json").write_text('{"success":true,"source":"candidate"}')
            result = worker.collect(root, {"success": False, "source": "supervisor"}, Mock())
        with zipfile.ZipFile(io.BytesIO(result)) as archive:
            self.assertEqual(len(archive.namelist()), len(set(archive.namelist())))
            self.assertEqual(json.loads(archive.read("worker.json")), {"success": False, "source": "supervisor"})
            self.assertEqual(json.loads(archive.read("benchmark/worker.json"))["source"], "candidate")

    def test_result_collection_stops_at_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "partial.json").write_text("{}")
            budget = Mock()
            budget.left.side_effect = TimeoutError()
            with self.assertRaises(TimeoutError):
                worker.collect(root, {}, budget)

    def test_cold_cache_and_full_verification_harness_are_not_replaced(self):
        command = worker.command({**window(), "suite": "large"}, 600)
        self.assertIn("linux-drop-caches", command)
        self.assertIn("benchmarks.run", command)
        self.assertIn("--property=NoNewPrivileges=yes", command)
        self.assertIn("--property=PrivateNetwork=yes", command)
        self.assertIn("--property=KillMode=control-group", command)
        self.assertIn("--property=RuntimeMaxSec=600", command)
        self.assertNotIn("--skip-validation", command)
        self.assertNotIn("--no-verify", command)


class PolicyTests(unittest.TestCase):
    def test_runtime_has_only_one_capacity_write_and_exact_mailbox_keys(self):
        import yaml
        root = Path(__file__).parent
        stack = yaml.safe_load((root / "stack.yaml").read_text())
        resources = stack["Resources"]
        asg = resources["WorkerGroup"]["Properties"]
        self.assertEqual([asg[k] for k in ("MinSize", "DesiredCapacity", "MaxSize")], ["0"] * 3)
        policy = resources["RuntimeRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        actions = [a for p in policy for a in ([p["Action"]] if isinstance(p["Action"], str) else p["Action"])]
        self.assertEqual(set(actions), {"autoscaling:SetDesiredCapacity", "autoscaling:DescribeAutoScalingGroups",
            "autoscaling:DescribeScheduledActions", "ec2:DescribeInstances", "s3:GetObject", "s3:PutObject"})
        self.assertEqual(policy[0]["Resource"], {"Fn::GetAtt": ["WorkerGroup", "AutoScalingGroupARN"]})
        for statement in policy[2:]:
            self.assertNotIn("*", json.dumps(statement["Resource"]))
        workflow = yaml.safe_load((root.parents[1] / ".github/workflows/aws-benchmark.yml").read_text())
        self.assertNotIn("id-token", workflow["permissions"])
        self.assertNotIn("id-token", workflow["jobs"]["build"]["permissions"])
        self.assertEqual(workflow["jobs"]["benchmark"]["environment"], "aws-benchmark")


if __name__ == "__main__":
    unittest.main()
