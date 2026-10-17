"""Offline tests: fake AWS responses only, including uncertain mutation outcomes."""

import argparse
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from infra.aws_benchmark import ami_build as ami


class FakeAws:
    def __init__(self, manifest):
        self.m = manifest
        self.calls = []
        self.clock = 1000
        self.rollback = False
        self.fail = None
        self.account = manifest["account"]
        self.marker = "RCP_AMI_READY:" + manifest["build_id"] + ":" + manifest["revision"]
        self.snapshot_state = "completed"
        self.state = "CREATE_IN_PROGRESS"
        self.root_remains = False
        self.instance_boot_mode = "legacy-bios"
        self.pending_reads = 0
        self.ami = dict(ImageId="ami-12345678901234567", OwnerId=self.account, Public=False,
                        State="available", Name=manifest["image_name"], Architecture="x86_64",
                        VirtualizationType="hvm", EnaSupport=True, RootDeviceType="ebs",
                        RootDeviceName="/dev/sda1", Tags=self.tags(), BlockDeviceMappings=[
                            {"DeviceName": "/dev/sda1", "Ebs": {
                                "VolumeSize": 16, "SnapshotId": "snap-12345678901234567"}}])

    def tags(self):
        return [{"Key": ami.TAG, "Value": self.m["build_id"]}]

    def sleep(self, seconds):
        self.clock += seconds

    def __call__(self, service, op, **request):
        self.calls.append((service, op, request))
        if op == self.fail:
            raise TimeoutError("response lost: " + op)
        if op == "get-caller-identity":
            return {"Account": self.account, "Arn": "arn:aws:sts::" + self.account + ":assumed-role/operator/session"}
        if op == "describe-images":
            image = copy.deepcopy(self.ami)
            if "ImageIds" in request:
                image.update(OwnerId="099720109477", Name="ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20261001")
            return {"Images": [image]}
        if op == "describe-instance-types":
            return {"InstanceTypes": [{"SupportedBootModes": ["legacy-bios", "uefi"]}] * 2}
        if op == "describe-subnets":
            return {"Subnets": [{"OwnerId": self.account, "VpcId": self.m["vpc"], "State": "available"}]}
        if op == "create-stack":
            return {"StackId": self.m["stack_id"]}
        if op == "describe-stacks":
            status = "ROLLBACK_IN_PROGRESS" if self.rollback and self.m["stage"] == "capture_requested" else self.state
            return {"Stacks": [{"StackId": self.m["stack_id"], "Tags": self.tags(),
                                "TimeoutInMinutes": 60, "DisableRollback": False, "StackStatus": status}]}
        if op == "describe-stack-resources":
            return {"StackResources": [{"LogicalResourceId": "Builder", "PhysicalResourceId": "i-12345678901234567"}]}
        if op == "describe-instances":
            if self.pending_reads:
                self.pending_reads -= 1
                state, mode = "pending", None
            else:
                state = "terminated" if self.state == "DELETE_COMPLETE" else "stopped"
                mode = self.instance_boot_mode
            return {"Reservations": [{"Instances": [{"Tags": self.tags(), "ImageId": self.m["base_ami"],
                     "InstanceType": "t3a.small", "State": {"Name": state},
                     "CurrentInstanceBootMode": mode,
                     "BlockDeviceMappings": [{"DeviceName": "/dev/sda1", "Ebs": {
                         "VolumeId": "vol-12345678901234567", "DeleteOnTermination": True}}]}]}]}
        if op == "get-console-output":
            return {"Output": "boot logs\n" + self.marker + "\nshutdown logs"}
        if op == "create-image":
            return {"ImageId": self.ami["ImageId"]}
        if op == "describe-snapshots":
            return {"Snapshots": [{"SnapshotId": "snap-12345678901234567", "OwnerId": self.account,
                     "Tags": self.tags(), "VolumeId": "vol-12345678901234567", "VolumeSize": 16,
                     "Encrypted": True, "State": self.snapshot_state}]}
        if op == "describe-image-attribute":
            return {"LaunchPermissions": []}
        if op == "delete-stack":
            self.state = "DELETE_COMPLETE"
            return {}
        if op == "describe-volumes":
            return {"Volumes": [dict(VolumeId="vol-12345678901234567")] if self.root_remains else []}
        raise AssertionError("unexpected AWS call: " + op)


class AmiBuildTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.m = dict(schema=1, build_id="a" * 32, revision="b" * 40, account="123456789012",
                      region="us-east-1", base_ami="ami-abcdef12345678901", vpc="vpc-abcdef12",
                      subnet="subnet-abcdef12", stack_name="rcp-ami-" + "a" * 32,
                      image_name="rcp-worker-" + "a" * 32, stage="prepared", snapshot_ids=[])
        self.m["stack_id"] = "arn:aws:cloudformation:us-east-1:123456789012:stack/" + self.m["stack_name"] + "/unique"
        body = (ami.HERE / "ami-builder.json").read_text()
        (self.directory / "template.json").write_text(body)
        self.m["template_sha256"] = hashlib.sha256(body.encode()).hexdigest()
        self.aws = FakeAws(self.m)

    def run_build(self):
        ami.run(self.aws, self.directory, self.m, now=lambda: self.aws.clock, sleep=self.aws.sleep)

    def operations(self):
        return [op for _, op, _ in self.aws.calls]

    def test_complete_capture_precedes_deletion_and_physical_cleanup(self):
        self.run_build()
        ops = self.operations()
        self.assertLess(ops.index("describe-snapshots"), ops.index("delete-stack"))
        self.assertLess(ops.index("describe-image-attribute"), ops.index("delete-stack"))
        self.assertLess(ops.index("delete-stack"), ops.index("describe-volumes"))
        self.assertEqual(self.m["stage"], "complete")
        create = next(r for _, op, r in self.aws.calls if op == "create-stack")
        self.assertEqual(create["TimeoutInMinutes"], 60)
        self.assertEqual(create["OnFailure"], "ROLLBACK")
        self.assertEqual(create["ClientRequestToken"], self.m["build_id"])
        capture = next(r for _, op, r in self.aws.calls if op == "create-image")
        self.assertTrue(capture["NoReboot"])
        self.assertEqual({t["ResourceType"] for t in capture["TagSpecifications"]}, {"image", "snapshot"})

    def test_wrong_account_cannot_launch(self):
        self.aws.account = "999999999999"
        with self.assertRaisesRegex(ValueError, "account differs"):
            self.run_build()
        self.assertNotIn("create-stack", self.operations())

    def test_changed_template_cannot_launch(self):
        (self.directory / "template.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "template changed"):
            self.run_build()
        self.assertEqual(self.aws.calls, [])

    def test_oversized_source_root_cannot_launch(self):
        self.aws.ami["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"] = 17
        with self.assertRaisesRegex(ValueError, "exceeds 16"):
            self.run_build()
        self.assertNotIn("create-stack", self.operations())

    def test_observed_boot_mode_mismatch_cannot_capture(self):
        self.aws.instance_boot_mode = "uefi"
        with self.assertRaisesRegex(ValueError, "observed builder boot mode mismatch"):
            self.run_build()
        self.assertNotIn("create-image", self.operations())

    def test_uefi_preferred_source_preserves_capture_boot_mode(self):
        self.aws.ami["BootMode"] = "uefi-preferred"
        self.aws.instance_boot_mode = "uefi"
        self.run_build()
        self.assertEqual(self.m["stage"], "complete")

    def test_pending_instance_can_wait_for_boot_mode_to_become_available(self):
        self.aws.pending_reads = 1
        self.run_build()
        self.assertEqual(self.m["stage"], "complete")
        self.assertEqual(self.operations().count("describe-instances"), 3)

    def test_cli_disables_retries_and_bounds_a_lost_mutation_response(self):
        with mock.patch.object(ami.subprocess, "run", side_effect=subprocess.TimeoutExpired("aws", 90)) as process:
            with self.assertRaises(subprocess.TimeoutExpired):
                ami.Aws("us-east-1")("ec2", "create-image", InstanceId="i-example", Name="example")
        self.assertEqual(process.call_count, 1)
        self.assertEqual(process.call_args.kwargs["env"]["AWS_MAX_ATTEMPTS"], "1")
        self.assertEqual(process.call_args.kwargs["timeout"], 90)
        self.assertIn("--no-cli-auto-prompt", process.call_args.args[0])

    def test_cli_error_preserves_service_reason_for_operator_recovery(self):
        failure = subprocess.CompletedProcess(["aws"], 254, stdout="", stderr="AccessDenied: example reason")
        with mock.patch.object(ami.subprocess, "run", return_value=failure):
            with self.assertRaisesRegex(ValueError, "cloudformation create-stack failed: AccessDenied"):
                ami.Aws("us-east-1")("cloudformation", "create-stack", StackName="example")

    def test_lost_launch_response_is_persisted_and_never_retried(self):
        self.aws.fail = "create-stack"
        with self.assertRaises(TimeoutError):
            self.run_build()
        saved = json.loads((self.directory / "manifest.json").read_text())
        self.assertEqual(saved["stage"], "create_requested")
        with self.assertRaisesRegex(ValueError, "one-shot"):
            self.run_build()
        self.assertEqual(self.operations().count("create-stack"), 1)

    def test_stopped_without_matching_marker_cannot_capture(self):
        for marker in ["", "RCP_AMI_READY:wrong:" + self.m["revision"]]:
            with self.subTest(marker=marker):
                self.setUp()
                self.aws.marker = marker
                with self.assertRaisesRegex(ValueError, "guest not ready"):
                    self.run_build()
                self.assertNotIn("create-image", self.operations())
                self.assertNotIn("delete-stack", self.operations())

    def test_guest_failure_cannot_capture(self):
        self.aws.marker += "\nRCP_AMI_FAILED:" + self.m["build_id"]
        with self.assertRaisesRegex(ValueError, "guest preparation failed"):
            self.run_build()
        self.assertNotIn("create-image", self.operations())

    def test_pending_snapshot_never_triggers_normal_deletion(self):
        self.aws.snapshot_state = "pending"
        with self.assertRaisesRegex(ValueError, "cutoff reached"):
            self.run_build()
        self.assertEqual(self.m["image_state"], "available")
        self.assertEqual(self.m["snapshot_ids"], ["snap-12345678901234567"])
        self.assertNotIn("delete-stack", self.operations())

    def test_lost_capture_response_reconciles_image_and_orphan_snapshot_ids(self):
        self.aws.fail = "create-image"
        with self.assertRaises(TimeoutError):
            self.run_build()
        self.assertEqual(self.m["stage"], "capture_requested")
        self.assertNotIn("image_id", self.m)
        ami.inventory(self.aws, self.m)
        self.assertEqual(self.m["image_id"], self.aws.ami["ImageId"])
        self.assertEqual(len(self.m["snapshot_ids"]), 1)
        self.assertNotIn("delete-stack", self.operations())
        self.assertEqual(self.operations().count("create-image"), 1)

    def test_rollback_during_capture_cannot_report_success_or_delete(self):
        self.aws.rollback = True
        with self.assertRaisesRegex(ValueError, "no longer creating"):
            self.run_build()
        self.assertNotIn("delete-stack", self.operations())
        self.assertEqual(self.m["stage"], "capture_requested")

    def test_lost_delete_response_preserves_recovery_state(self):
        self.aws.fail = "delete-stack"
        with self.assertRaises(TimeoutError):
            self.run_build()
        self.assertEqual(self.m["stage"], "delete_requested")
        self.assertIn("image_id", self.m)
        with self.assertRaisesRegex(ValueError, "one-shot"):
            self.run_build()

    def test_retained_root_volume_cannot_report_success(self):
        self.aws.root_remains = True
        with self.assertRaisesRegex(ValueError, "root volume remains"):
            self.run_build()
        self.assertNotEqual(self.m["stage"], "complete")

    def test_foreign_snapshot_cannot_trigger_deletion(self):
        self.aws.ami["BlockDeviceMappings"][0]["Ebs"]["SnapshotId"] = "snap-foreign"
        with self.assertRaisesRegex(ValueError, "snapshot mismatch"):
            self.run_build()
        self.assertNotIn("delete-stack", self.operations())

    def test_template_keeps_the_independent_cleanup_boundary(self):
        template = json.loads((ami.HERE / "ami-builder.json").read_text())
        self.assertEqual(set(template["Resources"]), {"SecurityGroup", "Builder"})
        builder = template["Resources"]["Builder"]
        self.assertEqual(builder["CreationPolicy"]["ResourceSignal"], {"Count": 1, "Timeout": "PT60M"})
        p = builder["Properties"]
        self.assertEqual(p["InstanceType"], "t3a.small")
        self.assertEqual(p["CreditSpecification"], {"CPUCredits": "standard"})
        self.assertEqual(p["InstanceInitiatedShutdownBehavior"], "stop")
        self.assertNotIn("IamInstanceProfile", p)
        self.assertNotIn("KeyName", p)
        self.assertEqual(p["MetadataOptions"]["HttpTokens"], "required")
        self.assertEqual(p["BlockDeviceMappings"][0]["Ebs"], {
            "VolumeSize": 16, "VolumeType": "gp3", "Encrypted": True, "DeleteOnTermination": True})
        self.assertEqual(template["Resources"]["SecurityGroup"]["Properties"]["SecurityGroupIngress"], [])

    def test_guest_defers_sanitation_until_cloud_final_and_never_signals_success(self):
        guest = (ami.HERE / "ami-guest.sh").read_text()
        subprocess.run(["bash", "-n", str(ami.HERE / "ami-guest.sh")], check=True)
        self.assertIn("systemd-run --no-block", guest)
        self.assertIn("After=cloud-final.service", guest)
        self.assertIn("SuccessAction=poweroff", guest)
        self.assertIn("FailureAction=poweroff", guest)
        self.assertLess(guest.index("cloud-init status --wait"), guest.index("cloud-init clean"))
        self.assertLess(guest.index("git ca-certificates"), guest.index("bash infra/aws_benchmark/install-worker.sh"))
        self.assertLess(guest.index("cloud-init clean"), guest.index("echo RCP_AMI_READY:"))
        self.assertNotIn("signal-resource", guest)
        self.assertNotIn("cfn-signal", guest)

    def test_prepare_is_offline_and_pins_source_into_reviewable_template(self):
        args = argparse.Namespace(account=self.m["account"], region=self.m["region"],
                                  base_ami=self.m["base_ami"], vpc=self.m["vpc"], subnet=self.m["subnet"],
                                  revision=self.m["revision"], directory=self.directory / "prepared")
        with mock.patch.object(ami.subprocess, "check_output", return_value=args.revision + "\n"):
            with mock.patch.object(ami.Aws, "__call__", side_effect=AssertionError("AWS forbidden")):
                ami.prepare(args)
        template = json.loads((args.directory / "template.json").read_text())
        guest = template["Resources"]["Builder"]["Properties"]["UserData"]["Fn::Base64"]
        self.assertIn(args.revision, guest)
        self.assertNotIn("__REVISION__", guest)
        self.assertLess(len(guest.encode()), 16384)


if __name__ == "__main__":
    unittest.main()
