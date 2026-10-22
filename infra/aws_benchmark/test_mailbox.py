"""Offline atomic ownership and cross-protocol arming failure paths."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import json
import threading
import unittest

from infra.aws_benchmark import common, durable_operator, mailbox, operator
from infra.aws_benchmark import test_durable_operator
from infra.aws_benchmark.test_safety import ClaimConflict, FakeCloud, NOW, window


class ClaimTests(unittest.TestCase):
    def test_conditional_claim_has_no_expiry_or_automatic_replacement(self):
        cloud = FakeCloud()
        owner = mailbox.claim(cloud.api["s3"], "bucket", "operator-window", window())
        self.assertEqual(owner, json.loads(cloud.objects[mailbox.OWNER_KEY]))
        self.assertEqual(set(owner), {"schema_version", "token", "bucket", "protocol", "run", "request_sha256"})
        self.assertEqual(cloud.api["s3"].put_object.call_args.kwargs["IfNoneMatch"], "*")
        with self.assertRaises(ClaimConflict):
            mailbox.claim(cloud.api["s3"], "bucket", "operator-window", window())
        cloud.api["s3"].get_object.assert_not_called()
        cloud.api["s3"].delete_object.assert_not_called()

    def test_concurrent_arms_have_exactly_one_owner_even_for_the_same_request(self):
        for second in ("operator-window", common.DIRECT_PROTOCOL):
            with self.subTest(second=second):
                cloud, gate, lock = FakeCloud(), threading.Barrier(2), threading.Lock()
                def put(**kwargs):
                    gate.wait(timeout=5)
                    with lock:
                        return cloud.put(**kwargs)
                cloud.api["s3"].put_object.side_effect = put
                def attempt(protocol):
                    try:
                        return mailbox.claim(cloud.api["s3"], "bucket", protocol, window())
                    except ClaimConflict:
                        return None
                with ThreadPoolExecutor(max_workers=2) as pool:
                    owners = list(pool.map(attempt, ("operator-window", second)))
                winners = [owner for owner in owners if owner is not None]
                self.assertEqual(len(winners), 1)
                self.assertEqual(winners[0], json.loads(cloud.objects[mailbox.OWNER_KEY]))
                cloud.api["s3"].delete_object.assert_not_called()

    def test_uncertain_accepted_claim_reconciles_only_its_exact_unique_document(self):
        cloud = FakeCloud()
        def put(**kwargs):
            cloud.put(**kwargs)
            raise TimeoutError("response lost")
        cloud.api["s3"].put_object.side_effect = put
        owner = mailbox.claim(cloud.api["s3"], "bucket", "operator-window", window())
        self.assertEqual(owner, json.loads(cloud.objects[mailbox.OWNER_KEY]))
        self.assertEqual(cloud.api["s3"].put_object.call_count, 1)
        self.assertEqual(cloud.api["s3"].get_object.call_count, 1)

    def test_uncertain_unaccepted_or_other_claim_never_assumes_ownership(self):
        for existing in (None, b'{"run":"123-1"}', b"malformed"):
            with self.subTest(existing=existing):
                cloud = FakeCloud()
                if existing is not None:
                    cloud.objects[mailbox.OWNER_KEY] = existing
                cloud.api["s3"].put_object.side_effect = TimeoutError("response lost")
                with self.assertRaises((TimeoutError, ValueError)):
                    mailbox.claim(cloud.api["s3"], "bucket", "operator-window", window())
                self.assertEqual(cloud.api["s3"].put_object.call_count, 1)
                cloud.api["s3"].delete_object.assert_not_called()

    def test_uncertain_put_cannot_join_another_invocations_identical_request(self):
        cloud = FakeCloud()
        first = mailbox.claim(cloud.api["s3"], "bucket", "operator-window", window())
        cloud.api["s3"].put_object.side_effect = TimeoutError("response lost")
        with self.assertRaisesRegex(ValueError, "belongs to another operator"):
            mailbox.claim(cloud.api["s3"], "bucket", "operator-window", window())
        self.assertEqual(json.loads(cloud.objects[mailbox.OWNER_KEY]), first)

    def test_unknown_conflict_and_failed_readback_never_retry_put(self):
        cloud = FakeCloud()
        cloud.api["s3"].put_object.side_effect = TimeoutError("response lost")
        cloud.api["s3"].get_object.side_effect = PermissionError("read denied")
        with self.assertRaises(TimeoutError):
            mailbox.claim(cloud.api["s3"], "bucket", "operator-window", window())
        self.assertEqual(cloud.api["s3"].put_object.call_count, 1)

    def test_existing_broad_unknown_or_owner_expiration_blocks_before_claim(self):
        filters = ({}, {"Prefix": ""}, {"Prefix": "control/"}, {"Prefix": mailbox.OWNER_KEY},
                   {"Tag": {"Key": "any", "Value": "value"}},
                   {"And": {"Prefix": "output/", "Tags": []}})
        for selected in filters:
            with self.subTest(selected=selected):
                cloud = FakeCloud()
                cloud.api["s3"].get_bucket_lifecycle_configuration.return_value = {"Rules": [
                    {"Status": "Enabled", "Filter": selected, "Expiration": {"Days": 7}}]}
                with self.assertRaisesRegex(ValueError, "expire ownership"):
                    mailbox.claim(cloud.api["s3"], "bucket", "operator-window", window())
                cloud.api["s3"].put_object.assert_not_called()

    def test_failed_lifecycle_read_never_claims(self):
        cloud = FakeCloud()
        cloud.api["s3"].get_bucket_lifecycle_configuration.side_effect = PermissionError()
        with self.assertRaises(PermissionError):
            mailbox.claim(cloud.api["s3"], "bucket", "operator-window", window())
        cloud.api["s3"].put_object.assert_not_called()


class ArmExclusivityTests(unittest.TestCase):
    def fixture(self):
        direct = test_durable_operator.OperatorTests()
        direct.setUp()
        cloud = FakeCloud()
        direct.service.api["s3"] = cloud.api["s3"]
        return direct, cloud, window(direct.now)

    def test_legacy_owner_blocks_direct_lease_and_mailbox_publication(self):
        direct, cloud, legacy = self.fixture()
        operator.arm(cloud.api, "group", "bucket", legacy, now=lambda: direct.now)
        saved = dict(cloud.objects)
        with self.assertRaises(ClaimConflict):
            durable_operator.arm(direct.service, direct.request, direct.quote)
        self.assertEqual(cloud.objects, saved)
        direct.service.api["dynamodb"].put_item.assert_not_called()

    def test_direct_owner_blocks_legacy_schedules_and_mailbox_publication(self):
        direct, cloud, legacy = self.fixture()
        durable_operator.arm(direct.service, direct.request, direct.quote)
        saved = dict(cloud.objects)
        with self.assertRaises(ClaimConflict):
            operator.arm(cloud.api, "group", "bucket", legacy, now=lambda: direct.now)
        self.assertEqual(cloud.objects, saved)
        cloud.api["autoscaling"].put_scheduled_update_group_action.assert_not_called()

    def test_direct_owner_blocks_legacy_after_worker_deadline_and_collection_horizon(self):
        direct, cloud, _ = self.fixture()
        durable_operator.arm(direct.service, direct.request, direct.quote)
        late = direct.now + timedelta(days=8)
        with self.assertRaises(ClaimConflict):
            operator.arm(cloud.api, "group", "bucket", window(late), now=lambda: late)
        cloud.api["autoscaling"].put_scheduled_update_group_action.assert_not_called()
        self.assertIn(mailbox.OWNER_KEY, cloud.objects)

    def test_direct_arm_refuses_unexpired_pre_migration_window_after_retaining_claim(self):
        direct, cloud, legacy = self.fixture()
        cloud.objects[common.WINDOW_KEY] = json.dumps(legacy).encode()
        with self.assertRaisesRegex(ValueError, "previous mailbox window"):
            durable_operator.arm(direct.service, direct.request, direct.quote)
        self.assertIn(mailbox.OWNER_KEY, cloud.objects)
        self.assertNotIn(common.STATUS_KEY, cloud.objects)
        direct.service.api["dynamodb"].put_item.assert_not_called()

    def test_failed_legacy_schedule_retains_claim_and_prevents_direct_arm(self):
        direct, cloud, legacy = self.fixture()
        cloud.fail = "close-" + legacy["run"]
        with self.assertRaises(RuntimeError):
            operator.arm(cloud.api, "group", "bucket", legacy, now=lambda: direct.now)
        self.assertIn(mailbox.OWNER_KEY, cloud.objects)
        with self.assertRaises(ClaimConflict):
            durable_operator.arm(direct.service, direct.request, direct.quote)
        direct.service.api["dynamodb"].put_item.assert_not_called()

    def test_failed_direct_lease_retains_claim_and_prevents_legacy_arm(self):
        direct, cloud, legacy = self.fixture()
        direct.service.api["dynamodb"].put_item.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            durable_operator.arm(direct.service, direct.request, direct.quote)
        self.assertIn(mailbox.OWNER_KEY, cloud.objects)
        with self.assertRaises(ClaimConflict):
            operator.arm(cloud.api, "group", "bucket", legacy, now=lambda: direct.now)
        cloud.api["autoscaling"].put_scheduled_update_group_action.assert_not_called()


if __name__ == "__main__":
    unittest.main()
