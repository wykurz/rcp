"""Pure durable contract versioning; admission must always remain historically readable."""

import copy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

from infra.aws_benchmark import common, durable


NOW = datetime(2026, 10, 21, 12, tzinfo=timezone.utc)


def request():
    pins = dict(region="us-east-1", account="123456789012", namespace="benchmark", bucket="bucket",
        lt_id="lt-" + "a" * 17, lt_version="9", ami="ami-" + "b" * 17, source="c" * 40,
        subnet="subnet-" + "d" * 17, security_group="sg-" + "e" * 17,
        profile_arn="arn:aws:iam::123456789012:instance-profile/worker")
    return durable.request(dict(run="123-1", baseline="a" * 40, candidate="a" * 40,
        suite="bulk-1g-metadata", controller="d" * 40), NOW,
        dict.fromkeys(common.BINARIES, common.digest(b"identical binary")), pins)


def bound_window(value):
    return durable.window(value, dict(instance_id="i-" + "1" * 17,
        launch_at=common.timestamp(NOW + timedelta(seconds=90)),
        attempt_id=value["run"] + "-1", attempt_started_at=common.timestamp(NOW),
        deadline=common.timestamp(NOW + timedelta(
            seconds=value["runtime_seconds"] - value["plan"]["shutdown_reserve_seconds"]))))


class DurableContractTests(unittest.TestCase):
    def test_current_aliases_cannot_admit_unrecorded_policy(self):
        value = request()
        original = common.durable_plan_contract(value["plan"])
        with patch.multiple(common, DURABLE_MAX_RUNTIME=20000, DURABLE_MAX_PROVISIONING=60000,
                            DIRECT_MAX_ATTEMPTS=999, BINARIES=common.BINARIES + ("candidate/extra",)):
            self.assertEqual(common.durable_plan_contract(value["plan"]), original)
            self.assertEqual(durable.validate(value), durable.validate(value, exact=False))
            changed = copy.deepcopy(value)
            changed["runtime_seconds"] = 20000
            changed["outer_deadline"] = common.timestamp(common.utc(value["capacity_deadline"]) + timedelta(seconds=20000))
            for exact in (True, False):
                with self.subTest(exact=exact), self.assertRaisesRegex(ValueError, "runtime does not fit"):
                    durable.validate(changed, exact=exact)

    def test_new_admission_uses_the_same_recorded_caps_pins_pairs_and_window_floor(self):
        value = request()
        v2 = common.durable_plan_contract(value["plan"], exact=False)
        v2.update(plan=dict(v2["plan"], version="bulk-metadata-plan-v2"), max_runtime=12000,
                  max_provisioning=43260, max_attempts=721, delivery_drain_seconds=1200,
                  binary_names=v2["binary_names"] + ("future-tool",),
                  pin_patterns=dict(v2["pin_patterns"], future="reviewed"))
        with patch.dict(common._RECORDED_DURABLE_CONTRACTS, {v2["plan"]["version"]: v2}), \
             patch.object(common, "DURABLE_PLAN", v2["plan"]):
            changed = copy.deepcopy(value)
            changed.update(plan=dict(v2["plan"]), runtime_seconds=12000,
                capacity_deadline=common.timestamp(NOW + timedelta(seconds=43260)),
                outer_deadline=common.timestamp(NOW + timedelta(seconds=43260 + 12000)))
            changed["pins"]["future"] = "reviewed"
            changed["sha256"].update({role + "/future-tool": "e" * 64 for role in ("baseline", "candidate")})
            self.assertEqual(durable.validate(changed), durable.validate(changed, exact=False))
            window = bound_window(changed)
            window["attempt_id"] = window["run"] + "-721"
            mailbox = {key: window[key] for key in ("run", "baseline", "candidate", "suite", "deadline")}
            mailbox["sha256"] = changed["sha256"]
            self.assertEqual(common.validate_request(mailbox, window), mailbox)
            for exact in (True, False):
                self.assertEqual(common.validate_durable_window(window, NOW + timedelta(seconds=90), 0, exact=exact),
                                 common.utc(window["deadline"]))
            self.assertEqual(durable.validate(value, exact=False), value)
            self.assertEqual(common.durable_plan_contract(value["plan"], exact=False)["delivery_drain_seconds"], 900)
            with self.assertRaisesRegex(ValueError, "not current"):
                durable.validate(value)
            changed["sha256"]["candidate/future-tool"] = "f" * 64
            for exact in (True, False):
                with self.subTest(exact=exact), self.assertRaisesRegex(ValueError, "A/A binary digests differ"):
                    durable.validate(changed, exact=exact)

    def test_recorded_entry_and_nested_policy_are_frozen(self):
        recorded = common._RECORDED_DURABLE_CONTRACTS[common.DURABLE_PLAN["version"]]
        for target, key, value in ((recorded, "max_runtime", 1), (recorded["plan"], "work_seconds", 1),
                                    (recorded["pin_patterns"], "source", ".*")):
            with self.subTest(key=key), self.assertRaises(TypeError):
                target[key] = value
        copied = common.durable_plan_contract(common.DURABLE_PLAN)
        copied["pin_patterns"]["source"] = ".*"
        copied["plan"]["work_seconds"] = 1
        self.assertEqual(common.durable_plan_contract(common.DURABLE_PLAN)["pin_patterns"]["source"], r"[0-9a-f]{40}")
        self.assertEqual(common.durable_plan_contract(common.DURABLE_PLAN)["plan"]["work_seconds"], 3104)

    def test_shared_minimum_runtime_rejects_the_same_boundary_in_request_and_window(self):
        value = request()
        floor = common.durable_minimum_runtime(value["plan"])
        self.assertEqual(floor, 10452)
        self.assertIs(durable.minimum_runtime, common.durable_minimum_runtime)
        for seconds in (floor - 1, floor):
            changed = copy.deepcopy(value)
            changed.update(runtime_seconds=seconds,
                outer_deadline=common.timestamp(common.utc(value["capacity_deadline"]) + timedelta(seconds=seconds)))
            window = bound_window(value)
            window.update(runtime_seconds=seconds, outer_deadline=changed["outer_deadline"],
                deadline=common.timestamp(NOW + timedelta(seconds=seconds - value["plan"]["shutdown_reserve_seconds"])))
            for validate in (lambda: durable.validate(changed),
                             lambda: common.validate_durable_window(window, NOW + timedelta(seconds=90), 0)):
                if seconds == floor:
                    validate()
                else:
                    with self.assertRaisesRegex(ValueError, "runtime does not fit"):
                        validate()

    def test_known_history_rejects_unknown_pin_and_cross_account_profile(self):
        value = request()
        for mutate, reason in ((lambda pins: pins.update(future="unknown"), "unexpected deployment pins"),
                (lambda pins: pins.update(profile_arn="arn:aws:iam::999999999999:instance-profile/worker"), "invalid pin"),
                (lambda pins: pins.update(lt_version="$Latest"), "invalid pin")):
            for exact in (True, False):
                changed = copy.deepcopy(value)
                mutate(changed["pins"])
                with self.subTest(exact=exact, reason=reason), self.assertRaisesRegex(ValueError, reason):
                    durable.validate(changed, exact=exact)


if __name__ == "__main__":
    unittest.main()
