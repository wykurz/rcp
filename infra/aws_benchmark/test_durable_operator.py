"""Offline direct operator funding, image and mutation-order regressions."""

import base64
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import yaml

from infra.aws_benchmark import durable
from infra.aws_benchmark.common import BINARIES, timestamp
from infra.aws_benchmark.durable_operator import arm, reservation, verify_image, verify_management
from infra.aws_benchmark.test_durable_coordinator import configuration, launch_data


class OperatorTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 21, 21, tzinfo=timezone.utc)
        self.config = configuration()
        self.request = durable.request(dict(run="123-1", baseline="a" * 40, candidate="a" * 40,
            controller="b" * 40, suite="bulk-1g-metadata"), self.now,
            {name: "f" * 64 for name in BINARIES}, self.config.pins())
        self.quote = dict(run="123-1", priced_at=timestamp(self.now), hourly_usd="4.608", ancillary_usd="2",
            shutdown_seconds=960, cap_usd="80", previous_reserved_usd="44", allocation_usd="18",
            cost_evidence="offline test fixture; not an approved reservation")
        self.calls = Mock()
        api = {name: Mock() for name in ("dynamodb", "s3", "ec2", "ssm", "kms")}
        for name, value in api.items():
            self.calls.attach_mock(value, name)
        api["dynamodb"].get_item.return_value = {}
        api["ssm"].get_service_setting.return_value = {"ServiceSetting": {
            "SettingId": "/ssm/managed-instance/default-ec2-instance-management-role",
            "SettingValue": "$None", "Status": "Default"}}
        self.service = SimpleNamespace(api=api, config=self.config, now=lambda: self.now,
            inventory=Mock(return_value=[]), fresh_guard=Mock(return_value=timestamp(self.now)))
        self.calls.attach_mock(self.service.inventory, "inventory")
        self.calls.attach_mock(self.service.fresh_guard, "fresh_guard")
        self.data = launch_data(self.config)
        template = yaml.safe_load(Path(__file__).with_name("stack.yaml").read_text())
        userdata = template["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"]["UserData"]["Fn::Base64"]["Fn::Sub"]
        userdata = userdata.replace("${WorkerProtocol}", "direct").replace("${Artifacts}", self.config.bucket).replace(
            "${AWS::Region}", self.config.region)
        self.data["UserData"] = base64.b64encode(userdata.encode()).decode()
        api["ec2"].describe_launch_template_versions.return_value = {"LaunchTemplateVersions": [
            {"VersionNumber": 9, "LaunchTemplateData": self.data}]}
        self.snapshot_id = "snap-" + "f" * 17
        self.image = dict(ImageId=self.config.ami, State="available", Public=False, Architecture="x86_64",
            VirtualizationType="hvm", EnaSupport=True, RootDeviceName="/dev/sda1", RootDeviceType="ebs", BlockDeviceMappings=[dict(DeviceName="/dev/sda1",
                Ebs=dict(SnapshotId=self.snapshot_id, Encrypted=True, VolumeSize=16))])
        api["ec2"].describe_images.return_value = {"Images": [self.image]}
        self.key_arn = "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
        self.snapshot = dict(SnapshotId=self.snapshot_id, Encrypted=True, VolumeSize=16, State="completed", KmsKeyId=self.key_arn)
        api["ec2"].describe_snapshots.return_value = {"Snapshots": [self.snapshot]}
        self.key = dict(Arn=self.key_arn, AWSAccountId=self.config.account, KeyManager="AWS", KeyState="Enabled",
                        KeyUsage="ENCRYPT_DECRYPT", KeySpec="SYMMETRIC_DEFAULT")
        api["kms"].describe_key.return_value = {"KeyMetadata": self.key}
        api["ec2"].describe_image_attribute.return_value = {"LaunchPermissions": []}
        api["ec2"].describe_subnets.return_value = {"Subnets": [dict(SubnetId=self.config.subnet,
            OwnerId=self.config.account, State="available", VpcId="vpc-1")]}
        api["ec2"].describe_security_groups.return_value = {"SecurityGroups": [dict(GroupId=self.config.security_group,
            OwnerId=self.config.account, VpcId="vpc-1", IpPermissions=[])]}

    def test_eighteen_dollar_fixture_covers_direct_guarded_exposure(self):
        projection = reservation(self.request, self.quote, self.now)
        self.assertEqual(projection["reserved_total_usd"], "62")
        self.assertEqual(projection["projection"]["guarded_failure_usd"], "17.0528")
        self.assertEqual(projection["projection"]["capacity_seconds"], 43200)

    def test_underfunded_attempt_is_rejected_before_any_cloud_operation(self):
        self.quote["allocation_usd"] = "16"
        with self.assertRaisesRegex(ValueError, "shutdown allowance"):
            arm(self.service, self.request, self.quote)
        self.assertFalse(self.calls.mock_calls)

    def test_approved_total_budget_cannot_be_silently_raised(self):
        self.quote["cap_usd"] = "60"
        with self.assertRaisesRegex(ValueError, "total approved budget"):
            reservation(self.request, self.quote, self.now)

    def test_stale_or_nonfinite_quote_fails_without_mutation(self):
        for change in ({"priced_at": timestamp(self.now - timedelta(days=2))}, {"allocation_usd": "NaN"},
                       {"hourly_usd": "Infinity"}, {"cost_evidence": ""}, {"run": "124-1"}, {"shutdown_seconds": 959}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                reservation(self.request, {**self.quote, **change}, self.now)

    def test_existing_lease_is_never_replaced(self):
        self.service.api["dynamodb"].get_item.return_value = {"Item": {"old": True}}
        with self.assertRaisesRegex(ValueError, "existing lease"):
            arm(self.service, self.request, self.quote)
        self.service.api["dynamodb"].put_item.assert_not_called()

    def test_fresh_independent_janitor_is_required_before_arming(self):
        self.service.fresh_guard.side_effect = ValueError("stale janitor")
        with self.assertRaisesRegex(ValueError, "stale janitor"):
            arm(self.service, self.request, self.quote)
        self.service.api["dynamodb"].put_item.assert_not_called()

    def test_live_managed_instance_blocks_new_lease(self):
        self.service.inventory.return_value = [{"State": {"Name": "running"}}]
        with self.assertRaisesRegex(ValueError, "physical managed"):
            arm(self.service, self.request, self.quote)
        self.service.api["dynamodb"].put_item.assert_not_called()

    def test_exclusive_lease_precedes_mailbox_publication_without_launch(self):
        result = arm(self.service, self.request, self.quote)
        names = [call[0] for call in self.calls.mock_calls]
        self.assertLess(names.index("dynamodb.put_item"), names.index("s3.put_object"))
        put = self.service.api["dynamodb"].put_item.call_args.kwargs
        self.assertEqual(put["ConditionExpression"], "attribute_not_exists(pk)")
        self.assertEqual(json.loads(put["Item"]["payload"]["S"])["attempts"], 0)
        self.assertEqual(result["state"], "armed-not-launched")
        self.service.api["ec2"].run_instances.assert_not_called()
        self.service.api["ec2"].terminate_instances.assert_not_called()

    def test_mailbox_failure_preserves_lease_without_launch_or_cleanup_side_effects(self):
        self.service.api["s3"].put_object.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            arm(self.service, self.request, self.quote)
        self.service.api["dynamodb"].put_item.assert_called_once()
        self.service.api["dynamodb"].delete_item.assert_not_called()
        self.service.api["ec2"].run_instances.assert_not_called()

    def test_enabled_instance_management_blocks_before_mutations(self):
        self.service.api["ssm"].get_service_setting.return_value["ServiceSetting"]["SettingValue"] = "management-role"
        with self.assertRaisesRegex(ValueError, "Host Management"):
            arm(self.service, self.request, self.quote)
        self.service.api["dynamodb"].put_item.assert_not_called()

    def test_management_read_uses_actual_region_and_account_without_asg(self):
        verify_management(self.service)
        self.service.api["ssm"].get_service_setting.assert_called_once_with(SettingId=
            "arn:aws:ssm:us-east-1:123456789012:servicesetting/ssm/managed-instance/default-ec2-instance-management-role")

    def test_reviewed_direct_launch_shape_is_accepted(self):
        verify_image(self.service)
        self.assertEqual([c.kwargs["KeyId"] for c in self.service.api["kms"].describe_key.call_args_list],
                         [self.key_arn, "alias/aws/ebs"])

    def test_retention_market_profile_and_metadata_drift_are_rejected(self):
        changes = [lambda d: d.update(DisableApiTermination=True), lambda d: d.update(InstanceMarketOptions={"MarketType": "spot"}),
            lambda d: d.update(InstanceRequirements={"VCpuCount": {"Min": 96}}),
            lambda d: d.update(InstanceInitiatedShutdownBehavior="stop"),
            lambda d: d["IamInstanceProfile"].update(Arn="arn:aws:iam::123456789012:instance-profile/admin"),
            lambda d: d["MetadataOptions"].update(HttpPutResponseHopLimit=2),
            lambda d: d["BlockDeviceMappings"][0]["Ebs"].update(DeleteOnTermination=False),
            lambda d: d["BlockDeviceMappings"][0]["Ebs"].update(KmsKeyId="alias/custom"),
            lambda d: d["NetworkInterfaces"][0].update(Groups=["sg-other"])]
        for change in changes:
            with self.subTest(change=change):
                original = copy.deepcopy(self.data)
                change(self.data)
                with self.assertRaises(ValueError):
                    verify_image(self.service)
                self.data.clear()
                self.data.update(original)

    def test_shared_image_and_customer_managed_snapshot_are_rejected(self):
        self.service.api["ec2"].describe_image_attribute.return_value = {"LaunchPermissions": [{"UserId": "999999999999"}]}
        with self.assertRaisesRegex(ValueError, "shared"):
            verify_image(self.service)
        self.service.api["ec2"].describe_image_attribute.return_value = {"LaunchPermissions": []}
        self.key["KeyManager"] = "CUSTOMER"
        with self.assertRaisesRegex(ValueError, "custom"):
            verify_image(self.service)

    def test_different_aws_managed_alias_key_is_rejected(self):
        self.service.api["kms"].describe_key.side_effect = [
            {"KeyMetadata": self.key}, {"KeyMetadata": {"Arn": self.key_arn + "-different"}}]
        with self.assertRaisesRegex(ValueError, "exact AWS-managed EBS key"):
            verify_image(self.service)

    def test_changed_guest_startup_is_rejected(self):
        self.data["UserData"] = base64.b64encode(b"#!/bin/bash\necho unsafe\n").decode()
        with self.assertRaisesRegex(ValueError, "guest startup"):
            verify_image(self.service)

    def test_unexpected_extra_image_volume_is_rejected(self):
        self.image["BlockDeviceMappings"].append(dict(DeviceName="/dev/sdb", Ebs={}))
        with self.assertRaisesRegex(ValueError, "exactly one EBS volume"):
            verify_image(self.service)

    def test_canonical_legacy_instance_store_mappings_preserve_single_root_validation(self):
        root = self.image["BlockDeviceMappings"][0]
        self.image["BlockDeviceMappings"] = [dict(DeviceName="/dev/sdb", VirtualName="ephemeral0"),
            root, dict(DeviceName="/dev/sdc", VirtualName="ephemeral1")]
        verify_image(self.service)
        self.service.api["ec2"].describe_snapshots.assert_called_once_with(
            SnapshotIds=[self.snapshot_id], OwnerIds=[self.config.account])

    def test_legacy_mappings_do_not_allow_duplicates_extra_ebs_or_unknown_devices(self):
        root = copy.deepcopy(self.image["BlockDeviceMappings"][0])
        cases = [[dict(DeviceName="/dev/sdb", Ebs={})],
            [dict(DeviceName="/dev/sda1", VirtualName="ephemeral0")],
            [dict(DeviceName="/dev/sdb", VirtualName="ephemeral0"),
             dict(DeviceName="/dev/sdc", VirtualName="ephemeral0")],
            [dict(DeviceName="/dev/sdb", VirtualName="ephemeral0", NoDevice="")],
            [dict(DeviceName="/dev/nvme0n1", VirtualName="ephemeral0")],
            [dict(DeviceName="/dev/sdb", VirtualName="ephemeral24")],
            [dict(DeviceName="/dev/sdb")]]
        for extras in cases:
            with self.subTest(extras=extras):
                self.image["BlockDeviceMappings"] = [copy.deepcopy(root), *extras]
                with self.assertRaises(ValueError):
                    verify_image(self.service)
        self.service.api["ec2"].describe_snapshots.assert_not_called()

    def test_legacy_mappings_do_not_allow_unencrypted_or_missing_root(self):
        self.image["BlockDeviceMappings"].append(dict(DeviceName="/dev/sdb", VirtualName="ephemeral0"))
        self.image["BlockDeviceMappings"][0]["Ebs"]["Encrypted"] = False
        with self.assertRaisesRegex(ValueError, "encrypted image root"):
            verify_image(self.service)
        self.image["BlockDeviceMappings"].pop(0)
        with self.assertRaisesRegex(ValueError, "root EBS mapping is missing"):
            verify_image(self.service)

    def test_inbound_security_group_access_is_rejected(self):
        self.service.api["ec2"].describe_security_groups.return_value["SecurityGroups"][0]["IpPermissions"] = [{"FromPort": 22}]
        with self.assertRaisesRegex(ValueError, "network ownership or isolation"):
            verify_image(self.service)


if __name__ == "__main__":
    unittest.main()
