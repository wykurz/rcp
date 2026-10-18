"""Offline tests: fake AWS responses only, including uncertain mutation outcomes."""

import argparse
import copy
from contextlib import redirect_stderr
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

from infra.aws_benchmark import ami_build as ami


# fields supplied from the account owner's live describe-images response in us-east-1
CANONICAL_IMAGE = json.loads((ami.HERE / "fixtures" / "canonical-ubuntu-20261017.json").read_text())


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
        self.source_image = {}
        self.not_found_reads = 0
        self.empty_reads = 0
        self.instance_error = None
        self.console_lag_reads = 0
        self.guest_stage = ""
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
                image.update(copy.deepcopy(self.source_image))
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
            if self.instance_error or self.not_found_reads:
                self.not_found_reads = max(0, self.not_found_reads - 1)
                code = self.instance_error or "InvalidInstanceID.NotFound"
                raise ami.AwsError(service, op, "An error occurred (" + code + ") when calling DescribeInstances: example")
            if self.empty_reads:
                self.empty_reads -= 1
                return {"Reservations": []}
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
            marker = self.marker
            if self.console_lag_reads:
                self.console_lag_reads -= 1
                marker = ""
            return {"Output": "boot logs\n" + self.guest_stage + "\n" + marker + "\nshutdown logs"}
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

    def run_build(self, progress=None):
        ami.run(self.aws, self.directory, self.m, now=lambda: self.aws.clock, sleep=self.aws.sleep,
                progress=progress or (lambda *args: None), clock=lambda: self.aws.clock)

    def run_cli(self):
        with mock.patch.object(ami.sys, "argv", ["ami_build", "run", "--directory", str(self.directory)]), \
             mock.patch.object(ami, "Aws", return_value=self.aws):
            ami.main()

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

    def test_progress_reports_stages_and_bounded_heartbeats_without_extra_reads(self):
        self.aws.pending_reads = 5
        output = io.StringIO()
        self.run_build(ami.Progress(output, clock=lambda: self.aws.clock))
        lines = output.getvalue().splitlines()
        guest = [line for line in lines if "[guest]" in line]
        self.assertEqual(len(guest), 3)
        for elapsed, line in zip([0, 30, 60], guest):
            self.assertIn(f"+{elapsed}s", line)
            self.assertRegex(line, r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ ")
            self.assertIn("builder=pending", line)
        for stage in ["preflight", "launch", "console", "capture", "snapshot", "cleanup", "verify", "complete"]:
            self.assertEqual(sum(f"[{stage}]" in line for line in lines), 1)
        self.assertIn("--instance-id i-12345678901234567", output.getvalue())
        self.assertEqual(self.operations().count("get-console-output"), 1)
        self.assertEqual(self.operations().count("describe-instances"), 7)

    def test_broken_progress_output_cannot_interrupt_capture_or_cleanup(self):
        for error in [BrokenPipeError("closed"), ValueError("closed stream")]:
            with self.subTest(error=error):
                self.setUp()
                stream = mock.Mock()
                stream.write.side_effect = error
                self.run_build(ami.Progress(stream, clock=lambda: self.aws.clock))
                self.assertEqual(self.m["stage"], "complete")
                self.assertEqual(self.operations().count("delete-stack"), 1)
                self.assertEqual(stream.write.call_count, 1)

    def test_progress_never_reports_complete_when_root_remains(self):
        self.aws.root_remains = True
        output = io.StringIO()
        with self.assertRaisesRegex(ValueError, "root volume remains"):
            self.run_build(ami.Progress(output, clock=lambda: self.aws.clock))
        self.assertNotIn("[complete]", output.getvalue())

    def test_progress_does_not_extend_guest_deadline(self):
        self.aws.pending_reads = 1000
        output = io.StringIO()
        with self.assertRaisesRegex(ValueError, "guest not ready"):
            self.run_build(ami.Progress(output, clock=lambda: self.aws.clock))
        self.assertEqual(self.aws.clock, self.m["submitted_at"] + 2400)
        self.assertNotIn("create-image", self.operations())
        self.assertEqual(len([line for line in output.getvalue().splitlines() if "[guest]" in line]), 81)

    def test_wrong_account_cannot_launch(self):
        self.aws.account = "999999999999"
        with self.assertRaisesRegex(ValueError, "account differs"):
            self.run_build()
        self.assertNotIn("create-stack", self.operations())

    def test_preflight_failure_reports_no_submission_and_preserves_safe_retry(self):
        ami.save(self.directory, self.m)
        self.aws.account = "999999999999"
        message = io.StringIO()
        with redirect_stderr(message), self.assertRaisesRegex(ValueError, "account differs"):
            self.run_cli()
        saved = json.loads((self.directory / "manifest.json").read_text())
        self.assertEqual(saved["stage"], "prepared")
        self.assertNotIn("submitted_at", saved)
        self.assertNotIn("create-stack", self.operations())
        self.assertIn("submitted no stack request", message.getvalue())
        self.assertIn("no rollback was armed", message.getvalue())
        self.assertNotIn("may remain and incur charges", message.getvalue())
        self.aws.account = self.m["account"]
        self.run_cli()
        saved = json.loads((self.directory / "manifest.json").read_text())
        self.assertEqual(saved["stage"], "complete")
        self.assertEqual(self.operations().count("create-stack"), 1)

    def test_uncertain_submission_warns_and_remains_nonretryable(self):
        ami.save(self.directory, self.m)
        self.aws.fail = "create-stack"
        message = io.StringIO()
        with redirect_stderr(message), self.assertRaises(TimeoutError):
            self.run_cli()
        saved = json.loads((self.directory / "manifest.json").read_text())
        self.assertEqual(saved["stage"], "create_requested")
        self.assertIn("submitted_at", saved)
        self.assertIn("may have been submitted", message.getvalue())
        self.assertNotIn("submitted no stack request", message.getvalue())
        with redirect_stderr(io.StringIO()), self.assertRaisesRegex(ValueError, "one-shot"):
            self.run_cli()
        self.assertEqual(self.operations().count("create-stack"), 1)

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

    def test_live_canonical_image_with_legacy_instance_store_mappings_completes(self):
        self.m["base_ami"] = CANONICAL_IMAGE["ImageId"]
        self.aws.source_image = copy.deepcopy(CANONICAL_IMAGE)
        self.run_build()
        self.assertEqual(self.m["stage"], "complete")
        source = {**self.aws.ami, **CANONICAL_IMAGE}
        root = ami.image_shape(source)
        self.assertEqual(root["VolumeSize"], 8)
        self.assertFalse(root["Encrypted"])
        source["BlockDeviceMappings"] = list(reversed(source["BlockDeviceMappings"]))
        self.assertEqual(ami.image_shape(source), root)

    def test_unsafe_source_mappings_are_rejected_before_launch(self):
        original = CANONICAL_IMAGE["BlockDeviceMappings"]
        variants = [
            original + [{"DeviceName": "/dev/sdd", "Ebs": {"VolumeSize": 1}}],
            original + [original[0]],
            original + [{"DeviceName": "/dev/sdb", "VirtualName": "ephemeral2"}],
            original + [{"DeviceName": "/dev/sdd", "VirtualName": "ephemeral0"}],
            original + [{"DeviceName": "/dev/sdd", "NoDevice": ""}],
            original + [{"DeviceName": "/dev/sdd", "VirtualName": "ephemeral2", "Ebs": {"VolumeSize": 1}}],
            original + [{"DeviceName": "/dev/sdd", "VirtualName": "ephemeral24"}],
            original + [{"DeviceName": "/dev/sda", "VirtualName": "ephemeral2"}],
            original + [{"DeviceName": "/dev/xvda", "VirtualName": "ephemeral2"}],
            [{**original[0], "NoDevice": ""}, *original[1:]],
            original[1:],
        ]
        for mappings in variants:
            with self.subTest(mappings=mappings):
                self.aws.source_image = {**CANONICAL_IMAGE, "BlockDeviceMappings": copy.deepcopy(mappings)}
                with self.assertRaises(ValueError):
                    self.run_build()
                self.assertEqual(self.m["stage"], "prepared")
                self.assertNotIn("create-stack", self.operations())

    def test_captured_image_can_retain_instance_store_references(self):
        self.aws.ami["BlockDeviceMappings"].extend(copy.deepcopy(CANONICAL_IMAGE["BlockDeviceMappings"][1:]))
        self.run_build()
        self.assertEqual(self.m["stage"], "complete")
        self.assertEqual(len(self.m["snapshot_ids"]), 1)

    def test_extra_captured_ebs_volume_cannot_trigger_cleanup(self):
        self.aws.source_image = copy.deepcopy(CANONICAL_IMAGE)
        self.aws.ami["BlockDeviceMappings"].append({"DeviceName": "/dev/sdd", "Ebs": {"VolumeSize": 1}})
        with self.assertRaisesRegex(ValueError, "exactly one EBS"):
            self.run_build()
        self.assertEqual(self.m["stage"], "capture_requested")
        self.assertNotIn("delete-stack", self.operations())

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

    def test_cli_reads_use_three_standard_attempts_within_the_same_timeout(self):
        success = subprocess.CompletedProcess(["aws"], 0, stdout="{}", stderr="")
        with mock.patch.dict(ami.os.environ, {"AWS_MAX_ATTEMPTS": "99", "AWS_RETRY_MODE": "adaptive"}):
            for service, operation in sorted(ami.READ_OPERATIONS):
                with self.subTest(operation=operation), mock.patch.object(ami.subprocess, "run", return_value=success) as process:
                    self.assertEqual(ami.Aws("us-east-1")(service, operation), {})
                    self.assertEqual(process.call_count, 1)
                    self.assertEqual(process.call_args.kwargs["timeout"], 90)
                    self.assertEqual(process.call_args.kwargs["env"]["AWS_MAX_ATTEMPTS"], "3")
                    self.assertEqual(process.call_args.kwargs["env"]["AWS_RETRY_MODE"], "standard")

    def test_cli_all_mutations_remain_single_attempt(self):
        for service, operation in [("cloudformation", "create-stack"), ("ec2", "create-image"),
                                   ("cloudformation", "delete-stack")]:
            with self.subTest(operation=operation), mock.patch.object(
                    ami.subprocess, "run", side_effect=subprocess.TimeoutExpired("aws", 90)) as process:
                with self.assertRaises(subprocess.TimeoutExpired):
                    ami.Aws("us-east-1")(service, operation)
                self.assertEqual(process.call_count, 1)
                self.assertEqual(process.call_args.kwargs["env"]["AWS_MAX_ATTEMPTS"], "1")

    def test_initial_instance_discovery_tolerates_not_found_and_empty_results(self):
        self.aws.not_found_reads = 2
        self.aws.empty_reads = 1
        self.run_build()
        self.assertEqual(self.m["stage"], "complete")
        self.assertEqual(self.operations().count("create-stack"), 1)
        self.assertEqual(self.operations().count("create-image"), 1)
        self.assertEqual(self.operations().count("describe-instances"), 5)
        self.assertEqual(self.aws.clock, self.m["submitted_at"] + 3 * ami.POLL)

    def test_initial_missing_instance_still_obeys_capture_cutoff(self):
        self.aws.not_found_reads = 1000
        with self.assertRaisesRegex(ValueError, "guest not ready"):
            self.run_build()
        self.assertEqual(self.aws.clock, self.m["submitted_at"] + 2400)
        self.assertNotIn("create-image", self.operations())

    def test_discovery_does_not_swallow_authorization_or_malformed_id_errors(self):
        for code in ["UnauthorizedOperation", "InvalidInstanceID.Malformed", "ExpiredToken"]:
            with self.subTest(code=code):
                self.setUp()
                self.aws.instance_error = code
                with self.assertRaises(ami.AwsError) as error:
                    self.run_build()
                self.assertEqual(error.exception.code, code)
                self.assertNotIn("create-image", self.operations())
                self.assertEqual(self.operations().count("describe-instances"), 1)

    def test_missing_instance_is_not_successful_cleanup_evidence(self):
        self.run_build()
        self.aws.not_found_reads = 1
        with self.assertRaises(ami.AwsError):
            ami.builder(self.aws, self.m)
        self.aws.empty_reads = 1
        with self.assertRaisesRegex(ValueError, "builder missing"):
            ami.builder(self.aws, self.m)

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
                with self.assertRaisesRegex(ValueError, "console-lag grace; last guest stage=unknown"):
                    self.run_build()
                self.assertEqual(self.aws.clock, self.m["submitted_at"] + ami.CONSOLE_GRACE)
                self.assertNotIn("create-image", self.operations())
                self.assertNotIn("delete-stack", self.operations())

    def test_empty_console_response_is_not_readiness_or_failure_evidence(self):
        self.m["instance_id"] = "i-12345678901234567"
        for console in [{}, {"Output": None}, {"Output": ""}]:
            with self.subTest(console=console):
                self.assertFalse(ami.ready(lambda *args, **kwargs: console, self.m,
                                           {"State": {"Name": "stopped"}}))
                self.assertNotIn("guest_stage", self.m)

    def test_stopped_builder_can_receive_readiness_after_console_lag(self):
        self.aws.console_lag_reads = 29  # more than the seven-minute lag observed in an account pilot
        self.run_build()
        self.assertEqual(self.m["stage"], "complete")
        self.assertEqual(self.operations().count("get-console-output"), 30)
        self.assertEqual(self.operations().count("create-image"), 1)

    def test_missing_marker_reports_only_last_bounded_stage(self):
        self.aws.marker = ""
        self.aws.guest_stage = "[guest:cloud-init]\n[guest:worker-install]\nraw unrelated output"
        with self.assertRaisesRegex(ValueError, "last guest stage=worker-install"):
            self.run_build()
        self.assertEqual(self.m["guest_stage"], "worker-install")
        self.assertEqual(self.aws.clock, self.m["submitted_at"] + ami.CONSOLE_GRACE)
        self.assertNotIn("create-image", self.operations())

    def test_console_grace_never_extends_original_capture_cutoff(self):
        self.aws.pending_reads = 130
        self.aws.marker = ""
        with self.assertRaisesRegex(ValueError, "guest not ready with 20 minutes left"):
            self.run_build()
        self.assertEqual(self.aws.clock, self.m["submitted_at"] + 2400)
        self.assertNotIn("create-image", self.operations())

    def test_console_grace_uses_monotonic_time_despite_wall_clock_rollback(self):
        self.aws.marker = ""
        wall = [self.aws.clock]
        def sleep(seconds):
            self.aws.sleep(seconds)
            wall[0] -= seconds
        with self.assertRaisesRegex(ValueError, "console-lag grace"):
            ami.run(self.aws, self.directory, self.m, now=lambda: wall[0], sleep=sleep,
                    progress=lambda *args: None, clock=lambda: self.aws.clock)
        self.assertEqual(self.aws.clock, self.m["submitted_at"] + ami.CONSOLE_GRACE)
        self.assertNotIn("create-image", self.operations())

    def test_failure_marker_stage_is_preserved_without_raw_console(self):
        self.aws.marker = "RCP_AMI_FAILED:" + self.m["build_id"] + " stage=worker-install line=42 exit=143"
        with self.assertRaisesRegex(ValueError, "guest preparation failed; last guest stage=worker-install"):
            self.run_build()
        self.assertEqual(self.m["guest_failure"], dict(stage="worker-install", line=42, exit=143))
        self.assertNotIn("create-image", self.operations())

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
        self.assertIn("StandardOutput=journal+console", guest)
        self.assertIn("StandardError=journal+console", guest)
        self.assertLess(guest.index("cloud-init status --wait"), guest.index("cloud-init clean"))
        self.assertLess(guest.index("git ca-certificates"), guest.index("bash infra/aws_benchmark/install-worker.sh"))
        self.assertLess(guest.index("cloud-init clean"), guest.index("echo RCP_AMI_READY:"))
        self.assertNotIn("signal-resource", guest)
        self.assertNotIn("cfn-signal", guest)

    def test_guest_failure_diagnostics_preserve_exit_status_and_fail_closed(self):
        guest = (ami.HERE / "ami-guest.sh").read_text().split("<<'PREPARE'\n", 1)[1]
        # execute only the initial checks with fake commands; no installation or host changes
        guest = guest.split("export DEBIAN_FRONTEND", 1)[0]
        console = self.directory / "console"
        release = self.directory / "os-release"
        release.write_text("ID=ubuntu\nVERSION_ID=24.04\n")
        guest = guest.replace("/dev/console", str(console)).replace("/etc/os-release", str(release))
        for cloud_status, python_status, stage in [(2, 0, "cloud-init"), (0, 31, "apt-quiesce")]:
            with self.subTest(stage=stage):
                stubs = (f"cloud-init() {{ return {cloud_status}; }}\n"
                         f"python3() {{ echo maintenance-reached; return {python_status}; }}\n"
                         "uname() { echo x86_64; }\n")
                result = subprocess.run(["bash"], input=stubs + guest, text=True, capture_output=True)
                self.assertEqual(result.returncode, cloud_status or python_status)
                marker = console.read_text()
                self.assertRegex(marker, f"RCP_AMI_FAILED:__BUILD_ID__ stage={stage} line=[0-9]+ exit={result.returncode}")
                self.assertNotIn("RCP_AMI_READY", marker)
                if cloud_status:
                    self.assertNotIn("maintenance-reached", result.stdout)

    def test_guest_term_trap_emits_failed_marker_and_exit_143(self):
        guest = (ami.HERE / "ami-guest.sh").read_text().split("<<'PREPARE'\n", 1)[1]
        # execute only trap definitions and signal this test shell; no preparation or host operations
        guest = guest.split("progress cloud-init", 1)[0]
        console = self.directory / "term-console"
        guest = guest.replace("/dev/console", str(console))
        result = subprocess.run(["bash"], input=guest + 'progress term-test\nkill -TERM "$BASHPID"\n',
                                text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 143)
        self.assertRegex(console.read_text(), r"RCP_AMI_FAILED:__BUILD_ID__ stage=term-test line=[0-9]+ exit=143")
        self.assertNotIn("RCP_AMI_READY", console.read_text())

    def test_outer_bootstrap_failure_emits_marker_before_stubbed_poweroff(self):
        guest = (ami.HERE / "ami-guest.sh").read_text().split("install -d", 1)[0]
        console = self.directory / "bootstrap-console"
        guest = guest.replace("/dev/console", str(console))
        stubs = 'systemctl() { echo "stubbed-$*"; }\n'
        result = subprocess.run(["bash"], input=stubs + guest + '(exit 37)\n',
                                text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 37)
        self.assertIn("stubbed-poweroff", result.stdout)
        self.assertRegex(console.read_text(), r"RCP_AMI_FAILED:__BUILD_ID__ stage=bootstrap line=[0-9]+ exit=37")

    def test_installer_reports_its_own_failed_stage_and_exit_status(self):
        script = (ami.HERE / "install-worker.sh").read_text().split("apt-get update", 1)[0]
        script = script.replace('cd "$(dirname "$0")/../.."', 'cd ' + str(self.directory))
        script = script.replace("/opt/rcp-benchmark", str(self.directory / "absent"))
        stubs = "id() { echo 0; }\nuname() { echo x86_64; }\npython3() { return 23; }\n"
        result = subprocess.run(["bash"], input=stubs + script, text=True, capture_output=True)
        self.assertEqual(result.returncode, 23)
        self.assertRegex(result.stderr, r"RCP_INSTALL_FAILED stage=apt-quiesce line=[0-9]+ exit=23")

    def test_private_checkout_installs_readable_nonwritable_harness_modules(self):
        source = self.directory / "source"
        shutil.copytree(ami.HERE.parent.parent / "benchmarks", source / "benchmarks")
        shutil.copytree(ami.HERE, source / "infra" / "aws_benchmark")
        for path in source.rglob("*"):
            path.chmod(0o700 if path.is_dir() else 0o600)
        destination = self.directory / "installed"
        (destination / "infra" / "aws_benchmark").mkdir(parents=True)
        (destination / "bin").mkdir()
        script = (ami.HERE / "install-worker.sh").read_text()
        script = script.split("cp -r benchmarks", 1)[1].split("progress complete", 1)[0]
        script = ("cp -r benchmarks" + script).replace("/opt/rcp-benchmark", str(destination))
        stubs = ("set -euo pipefail\numask 077\nprogress() { :; }\npython3() { :; }\n"
                 "git() { echo pinned-revision; }\nchown() { :; }\n"
                 'runuser() { shift 3; "$@"; }\n')
        subprocess.run(["bash"], input=stubs + script, cwd=source, text=True, check=True)
        for tree in [destination / "benchmarks", destination / "infra"]:
            for path in [tree, *tree.rglob("*")]:
                mode = path.stat().st_mode
                self.assertEqual(mode & 0o022, 0, str(path))
                self.assertEqual(mode & (0o005 if path.is_dir() else 0o004),
                                 0o005 if path.is_dir() else 0o004, str(path))

    def test_prepare_uses_committed_blobs_despite_staged_and_unstaged_edits(self):
        source = self.directory / "repository" / "infra" / "aws_benchmark"
        source.mkdir(parents=True)
        names = ["ami-builder.json", "ami-guest.sh", "maintenance.py"]
        committed = {name: (ami.HERE / name).read_text() for name in names}
        for name, body in committed.items():
            (source / name).write_text(body)
        def git(*arguments):
            return subprocess.check_output(["git", "-c", "core.hooksPath=/dev/null",
                                            "-c", "commit.gpgsign=false", "-c", "user.name=Offline Test",
                                            "-c", "user.email=offline@example.invalid", *arguments],
                                           cwd=source.parent.parent, text=True, stderr=subprocess.PIPE).strip()
        git("init", "-q")
        git("add", ".")
        git("commit", "-qm", "Committed AMI fixture")
        revision = git("rev-parse", "HEAD")
        edited_template = json.loads(committed["ami-builder.json"])
        edited_template["Resources"]["SecurityGroup"]["Properties"]["SecurityGroupIngress"] = [
            {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "CidrIp": "0.0.0.0/0"}]
        (source / "ami-builder.json").write_text(json.dumps(edited_template))
        git("add", "infra/aws_benchmark/ami-builder.json")
        (source / "ami-guest.sh").write_text("UNREVIEWED_GUEST\n")
        (source / "maintenance.py").write_text("UNREVIEWED_MAINTENANCE\n")
        self.assertTrue(git("status", "--porcelain"))
        args = argparse.Namespace(account=self.m["account"], region=self.m["region"],
                                  base_ami=self.m["base_ami"], vpc=self.m["vpc"], subnet=self.m["subnet"],
                                  revision=revision, directory=self.directory / "prepared")
        with mock.patch.object(ami, "HERE", source), mock.patch.object(
                ami.Aws, "__call__", side_effect=AssertionError("AWS forbidden")):
            ami.prepare(args)
        template = json.loads((args.directory / "template.json").read_text())
        guest = template["Resources"]["Builder"]["Properties"]["UserData"]["Fn::Base64"]
        self.assertEqual(template["Resources"]["SecurityGroup"]["Properties"]["SecurityGroupIngress"], [])
        self.assertIn(args.revision, guest)
        self.assertNotIn("__REVISION__", guest)
        self.assertNotIn("__MAINTENANCE_SOURCE__", guest)
        self.assertNotIn("UNREVIEWED", guest)
        self.assertIn(committed["maintenance.py"], guest)
        self.assertLess(guest.index("python3 /run/rcp-ami-build/maintenance.py --quiesce"),
                        guest.index("apt-get update"))
        subprocess.run(["bash", "-n"], input=guest, text=True, check=True)
        self.assertLess(len(guest.encode()), 16384)
        manifest = json.loads((args.directory / "manifest.json").read_text())
        self.assertEqual(manifest["revision"], revision)
        self.assertEqual(manifest["template_sha256"],
                         hashlib.sha256((args.directory / "template.json").read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
