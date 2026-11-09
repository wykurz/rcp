"""Independent offline negative tests for extended manual admission boundaries."""

import copy
from dataclasses import asdict, replace
from datetime import timedelta
import unittest
from unittest.mock import Mock

from .production_handler import read_policy, _LEGACY_POLICY_FIELDS, _FILE_SET_POLICY_FIELDS, _POLICY_FIELDS
from .production_policy import canonical, SCHEDULE_MAIN_AA, MAX_PRICE_AGE_SECONDS
from .test_full_file_set_admission import FULL, configured, approve
from .test_production_admission import Cloud
from .test_production_policy import NOW, inputs, policy


class FullFileSetPolicyReviewTests(unittest.TestCase):
    def assert_no_paid_admission(self, cloud):
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "64000000")
        cloud.reader.read.assert_not_called()
        cloud.api["s3"].generate_presigned_url.assert_not_called()
        cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_exact_price_boundary_reserves_fifty_and_one_more_microdollar_is_rejected_early(self):
        exact = configured(hourly_microusd=3_600_000, ancillary_microusd=20_240_000)
        self.assertEqual(approve(exact)["reserve_microusd"], 50_000_000)
        cloud = Cloud()
        cloud.policy = replace(exact, ancillary_microusd=20_240_001)
        with self.assertRaisesRegex(ValueError, "fixed qualification reservation"):
            cloud.service.begin("not-a-token", inputs(suite=FULL))
        self.assert_no_paid_admission(cloud)

    def test_caller_cannot_supply_its_own_runtime_capacity_or_price(self):
        for field in ("runtime_seconds", "manual_runtime_seconds", "capacity_seconds", "reserve_microusd"):
            with self.subTest(field=field):
                cloud = Cloud()
                cloud.policy = configured()
                value = inputs(suite=FULL)
                value[field] = 1
                with self.assertRaises(ValueError):
                    cloud.service.begin("not-a-token", value)
                self.assert_no_paid_admission(cloud)

    def test_runtime_extension_cannot_be_moved_to_global_runtime_field(self):
        for runtime in (28_800, 28_800.0, True):
            with self.subTest(runtime=runtime), self.assertRaises(ValueError):
                configured(runtime_seconds=runtime)
        with self.assertRaises(ValueError):
            policy(manual_runtime_seconds=28_800)

    def test_stale_price_or_missing_manual_authority_never_reads_app_proof(self):
        for value in (replace(configured(), priced_at=(NOW - timedelta(seconds=MAX_PRICE_AGE_SECONDS + 1)).isoformat()), policy()):
            with self.subTest(policy=value):
                cloud = Cloud()
                cloud.policy = value
                with self.assertRaises(ValueError):
                    cloud.service.begin("not-a-token", inputs(suite=FULL))
                self.assert_no_paid_admission(cloud)

    def test_disabled_upload_replay_and_seal_never_read_app_proof(self):
        cloud = Cloud()
        cloud.policy = configured()
        cloud.service.begin("not-a-token", inputs(suite=FULL))
        reserved = copy.deepcopy(cloud.ddb.items["budget#total"])
        cloud.policy = replace(cloud.policy, enabled=False)
        cloud.reader.read.reset_mock()
        cloud.api["s3"].generate_presigned_url.reset_mock()
        for operation in (lambda: cloud.service.begin("not-a-token", inputs(suite=FULL)),
                          lambda: cloud.service.seal_and_start("not-a-token")):
            with self.assertRaisesRegex(ValueError, "disabled"):
                operation()
        cloud.reader.read.assert_not_called()
        cloud.api["s3"].generate_presigned_url.assert_not_called()
        cloud.api["stepfunctions"].start_execution.assert_not_called()
        self.assertEqual(cloud.ddb.items["budget#total"], reserved)

    def test_only_exact_historical_and_current_envelopes_are_readable(self):
        self.assertEqual(tuple(map(len, (_LEGACY_POLICY_FIELDS, _FILE_SET_POLICY_FIELDS, _POLICY_FIELDS))), (17, 18, 19))
        base = asdict(configured())
        for names in (_LEGACY_POLICY_FIELDS, _FILE_SET_POLICY_FIELDS, _POLICY_FIELDS):
            value = {key: copy.deepcopy(base[key]) for key in names}
            if names != _POLICY_FIELDS and "manual_suites" in value:
                value["manual_suites"] = ["bulk-1g-metadata", "bulk-100m-metadata"]
            api = Mock()
            payload = canonical(value).decode()
            api.get_item.return_value = dict(Item={"pk": {"S": "policy"}, "version": {"S": base["version"]},
                "enabled": {"BOOL": True}, "payload": {"S": payload}})
            parsed = read_policy(api, "production-table", "483465")
            self.assertEqual(parsed.manual_runtime_seconds, 28_800 if names == _POLICY_FIELDS else 0)
            self.assertEqual(api.get_item.return_value["Item"]["payload"]["S"], payload)
        for omitted in ("manual_suites", "runtime_seconds", "owner_id"):
            value = dict(base)
            del value[omitted]
            api.get_item.return_value["Item"]["payload"]["S"] = canonical(value).decode()
            with self.subTest(omitted=omitted), self.assertRaises(ValueError):
                read_policy(api, "production-table", "483465")

    def test_explicit_maintainer_keeps_three_hour_authority_but_cannot_use_eight_hours(self):
        value = configured(manual_actor_ids=("55",))
        ordinary = approve(value, "bulk-1g-metadata", actor_id="55")
        self.assertEqual((ordinary["runtime_seconds"], ordinary["capacity_seconds"]), (10_800, 43_200))
        with self.assertRaisesRegex(ValueError, "owner"):
            approve(value, actor_id="55")
        self.assertEqual(approve(value)["runtime_seconds"], 28_800)

    def test_manual_full_set_does_not_need_to_allowlist_the_scheduled_suite(self):
        value = configured(manual_suites=(FULL,), schedule_slots=((3, 19, 30),), scheduled_baseline=SCHEDULE_MAIN_AA)
        approved = approve(value, "bulk-1g-metadata", event_name="schedule")
        self.assertEqual((approved["runtime_seconds"], approved["capacity_seconds"], approved["reserve_microusd"]),
            (10_800, 43_200, 20_000_000))
        self.assertEqual(approve(value)["reserve_microusd"], 50_000_000)


if __name__ == "__main__":
    unittest.main()
