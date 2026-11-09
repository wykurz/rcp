"""Full-file-set admission uses only fixed in-memory APIs; no service calls."""

import copy
from dataclasses import asdict
from datetime import timedelta
import unittest
from unittest.mock import Mock

from .common import durable_plan_for_suite
from .production_handler import read_policy, _LEGACY_POLICY_FIELDS
from .production_policy import WorkflowRun, authorize, canonical, SCHEDULE_MAIN_AA
from .test_production_admission import Cloud
from .test_production_policy import NOW, identity, inputs, policy, run_json


FULL = "all-filesets-metadata"


def configured(**updates):
    return policy(**(dict(manual_suites=("bulk-1g-metadata", FULL),
                         manual_runtime_seconds=28_800, monthly_cap_microusd=200_000_000,
                         ancillary_microusd=4_947_200) | updates))


def approve(configured_policy, suite=FULL, **identity_updates):
    who = identity(**identity_updates)
    proof = WorkflowRun.verify(run_json(who), who, NOW)
    return authorize(configured_policy, who, proof, inputs(suite=suite), NOW)


class FullFileSetAdmissionTests(unittest.TestCase):
    def test_full_set_reserves_once_and_seals_the_shorter_capacity_window(self):
        cloud = Cloud()
        cloud.policy = configured()
        cloud.ddb.items["policy"]["version"]["S"] = cloud.policy.version
        first = cloud.service.begin("not-a-token", inputs(suite=FULL))
        second = cloud.service.begin("not-a-token", inputs(suite=FULL))
        self.assertEqual(first["reserve_microusd"], 50_000_000)
        self.assertEqual(second["reserve_microusd"], 50_000_000)
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "114000000")
        record = cloud.service.store(cloud.who).read()
        request = record.state["request"]
        self.assertEqual(request["runtime_seconds"], 28_800)
        self.assertEqual(request["plan"], durable_plan_for_suite(FULL))
        self.assertEqual(request["capacity_deadline"], (NOW + timedelta(hours=2)).isoformat())
        self.assertEqual(request["outer_deadline"], (NOW + timedelta(hours=10)).isoformat())
        cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_extended_manual_policy_does_not_extend_or_reprice_regular_schedule(self):
        configured_policy = configured(schedule_slots=((3, 19, 30),), scheduled_baseline=SCHEDULE_MAIN_AA)
        approved = approve(configured_policy, "bulk-1g-metadata", event_name="schedule")
        self.assertEqual(approved["runtime_seconds"], 10_800)
        self.assertEqual(approved["capacity_seconds"], 43_200)
        self.assertEqual(approved["reserve_microusd"], 20_000_000)
        self.assertEqual(approved["schedule_slot"], "2026-10-22T19:30Z")

    def test_scheduled_full_set_and_other_actors_cannot_inherit_manual_authority(self):
        for updates in (dict(event_name="schedule"), dict(actor_id="55")):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                approve(configured(), **updates)

    def test_full_set_requires_both_suite_and_extended_runtime_opt_in(self):
        for updates in (dict(manual_runtime_seconds=0),
                        dict(manual_suites=("bulk-1g-metadata",)),
                        dict(manual_runtime_seconds=True),
                        dict(manual_runtime_seconds=28_799),
                        dict(manual_runtime_seconds=28_801)):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                configured(**updates)
        with self.assertRaises(ValueError):
            approve(policy())

    def test_all_individual_new_sets_retain_the_ordinary_runtime(self):
        for suite in ("bulk-100m-metadata", "bulk-1m-metadata", "tiny-1k-metadata"):
            with self.subTest(suite=suite):
                value = policy(manual_suites=("bulk-1g-metadata", suite))
                approved = approve(value, suite)
                self.assertEqual((approved["runtime_seconds"], approved["capacity_seconds"]), (10_800, 43_200))

    def test_prices_exceeding_fixed_qualification_reservation_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "fixed qualification reservation"):
            approve(configured(hourly_microusd=10_000_000))
        with self.assertRaises(ValueError):
            approve(configured(monthly_cap_microusd=49_999_999))

    def test_disabled_full_set_never_reads_app_backed_run_proof_or_issues_uploads(self):
        cloud = Cloud()
        cloud.policy = configured(enabled=False)
        with self.assertRaisesRegex(ValueError, "disabled"):
            cloud.service.begin("not-a-token", inputs(suite=FULL))
        cloud.reader.read.assert_not_called()
        cloud.api["s3"].generate_presigned_url.assert_not_called()
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "64000000")

    def test_two_suite_limit_prevents_implicitly_enabling_every_individual_set(self):
        with self.assertRaises(ValueError):
            configured(manual_suites=("bulk-1g-metadata", "bulk-1m-metadata", FULL))

    def test_explicit_policy_envelopes_preserve_both_historical_authorities(self):
        current = asdict(configured())
        for names, expected in ((_LEGACY_POLICY_FIELDS, ("bulk-1g-metadata",)),
                                (_LEGACY_POLICY_FIELDS | {"manual_suites"},
                                 ("bulk-1g-metadata", "bulk-100m-metadata"))):
            value = {key: copy.deepcopy(current[key]) for key in names}
            if "manual_suites" in value:
                value["manual_suites"] = list(expected)
            payload = canonical(value).decode()
            api = Mock()
            api.get_item.return_value = dict(Item={"pk": {"S": "policy"},
                "version": {"S": value["version"]}, "enabled": {"BOOL": True}, "payload": {"S": payload}})
            parsed = read_policy(api, "production-table", "483465")
            self.assertEqual(parsed.manual_suites, expected)
            self.assertEqual(parsed.manual_runtime_seconds, 0)
        api.get_item.return_value["Item"]["payload"]["S"] = canonical(current).decode()
        parsed = read_policy(api, "production-table", "483465")
        self.assertEqual(parsed.manual_runtime_seconds, 28_800)

    def test_partial_unknown_envelope_does_not_acquire_new_authority(self):
        value = asdict(configured())
        value.pop("manual_suites")
        api = Mock()
        api.get_item.return_value = dict(Item={"pk": {"S": "policy"}, "version": {"S": value["version"]},
            "enabled": {"BOOL": True}, "payload": {"S": canonical(value).decode()}})
        with self.assertRaises(ValueError):
            read_policy(api, "production-table", "483465")


if __name__ == "__main__":
    unittest.main()
