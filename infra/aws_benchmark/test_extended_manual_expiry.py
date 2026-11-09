"""Extended manual expiry gates new reservations, not an already admitted run."""
from dataclasses import asdict, replace
from datetime import timedelta
import copy
import unittest
from unittest.mock import Mock

from .production_handler import read_policy, _EXTENDED_RUNTIME_POLICY_FIELDS
from .production_policy import WorkflowRun, authorize, canonical, SCHEDULE_MAIN_AA
from .test_full_file_set_admission import FULL, configured
from .test_production_admission import Cloud
from .test_production_policy import NOW, identity, inputs, run_json


class ExtendedManualExpiryTests(unittest.TestCase):
    def assert_no_reservation(self, cloud):
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "64000000")
        cloud.reader.read.assert_not_called()
        cloud.api["s3"].generate_presigned_url.assert_not_called()
        cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_missing_reached_and_elapsed_expiry_reject_before_app_or_budget_use(self):
        for end in ("", NOW.isoformat(), (NOW - timedelta(seconds=1)).isoformat()):
            with self.subTest(end=end):
                cloud = Cloud()
                cloud.policy = configured(extended_manual_not_after=end)
                with self.assertRaisesRegex(ValueError, "admission window"):
                    cloud.service.begin("not-a-token", inputs(suite=FULL))
                self.assert_no_reservation(cloud)

    def test_accepted_request_clock_is_rechecked_after_run_proof(self):
        cloud = Cloud()
        end = NOW + timedelta(seconds=1)
        cloud.policy = configured(extended_manual_not_after=end.isoformat())
        cloud.service.now = Mock(side_effect=[NOW, NOW, NOW, end])
        with self.assertRaisesRegex(ValueError, "admission window"):
            cloud.service.begin("not-a-token", inputs(suite=FULL))
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "64000000")
        cloud.reader.read.assert_called_once()
        cloud.api["s3"].generate_presigned_url.assert_not_called()

    def test_expired_full_opt_in_leaves_regular_schedule_authority_and_price_unchanged(self):
        p = configured(extended_manual_not_after=(NOW - timedelta(seconds=1)).isoformat(),
                       schedule_slots=((3, 19, 30),), scheduled_baseline=SCHEDULE_MAIN_AA)
        who = identity(event_name="schedule")
        proof = WorkflowRun.verify(run_json(who), who, NOW)
        approved = authorize(p, who, proof, inputs(), NOW)
        self.assertEqual((approved["runtime_seconds"], approved["capacity_seconds"],
                          approved["reserve_microusd"]), (10_800, 43_200, 20_000_000))

    def test_pre_expiry_admission_can_finish_upload_and_seal_after_expiry(self):
        cloud = Cloud()
        cloud.policy = configured(extended_manual_not_after=(NOW + timedelta(seconds=1)).isoformat())
        initial = cloud.service.begin("not-a-token", inputs(suite=FULL))
        cloud.now += timedelta(seconds=2)
        replay = cloud.service.begin("not-a-token", inputs(suite=FULL))
        self.assertEqual(replay["outer_deadline"], initial["outer_deadline"])
        cloud.upload()
        sealed = cloud.seal()
        self.assertEqual(sealed["outer_deadline"], initial["outer_deadline"])
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "114000000")
        cloud.api["stepfunctions"].start_execution.assert_called_once()

    def test_delayed_commit_keeps_the_pre_expiry_native_horizon(self):
        cloud = Cloud()
        cloud.policy = configured(extended_manual_not_after=(NOW + timedelta(seconds=1)).isoformat())
        cloud.ddb.before_write = lambda: setattr(cloud, "now", NOW + timedelta(seconds=2))
        accepted = cloud.service.begin("not-a-token", inputs(suite=FULL))
        record = cloud.service.store(cloud.who).read()
        self.assertEqual(record.state["request"]["created_at"], NOW.isoformat())
        self.assertEqual(accepted["outer_deadline"], (NOW + timedelta(hours=10)).isoformat())
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "114000000")

    def test_legacy_extended_envelope_cannot_inherit_unbounded_new_admission(self):
        old = {k: copy.deepcopy(v) for k, v in asdict(configured()).items()
               if k in _EXTENDED_RUNTIME_POLICY_FIELDS}
        api = Mock()
        api.get_item.return_value = {"Item": {"pk": {"S": "policy"}, "version": {"S": old["version"]},
            "enabled": {"BOOL": True}, "payload": {"S": canonical(old).decode()}}}
        parsed = read_policy(api, "production-table", "483465")
        self.assertEqual(parsed.extended_manual_not_after, "")
        cloud = Cloud()
        cloud.policy = parsed
        with self.assertRaisesRegex(ValueError, "admission window"):
            cloud.service.begin("not-a-token", inputs(suite=FULL))
        self.assert_no_reservation(cloud)
        self.assertEqual(parsed.reservation(NOW), 20_000_000)

    def test_expiry_must_be_a_canonical_utc_timestamp_or_empty(self):
        for invalid in (True, None, 1, "2026-10-22T20:00:00", "2026-10-22T20:00:00Z",
                        "2026-10-22T21:00:00+01:00", "x" * 41):
            with self.subTest(invalid=invalid), self.assertRaises((TypeError, ValueError)):
                configured(extended_manual_not_after=invalid)
        configured(extended_manual_not_after="")

    def test_caller_cannot_supply_an_expiry(self):
        cloud = Cloud()
        cloud.policy = configured()
        supplied = inputs(suite=FULL) | {"extended_manual_not_after": "2099-01-01T00:00:00+00:00"}
        with self.assertRaises(ValueError):
            cloud.service.begin("not-a-token", supplied)
        self.assert_no_reservation(cloud)


if __name__ == "__main__":
    unittest.main()
