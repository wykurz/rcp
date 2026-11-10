"""Deployment cutover interleavings without any AWS or GitHub requests."""

import copy
import json
from dataclasses import replace
import unittest
from unittest.mock import Mock, patch

from . import production_handler as handler
from .production_policy import canonical, launch_pins_sha256
from .production_store import ReconciliationRequired
from .test_production_admission import Cloud
from .test_production_handler import environment, policy_item
from .test_production_policy import NOW, inputs, policy


class DeploymentPolicyTests(unittest.TestCase):
    def setUp(self):
        self.cloud = Cloud()
        self.pins = self.cloud.config.pins()

    def row(self, *, pins=None, version="deployment-new"):
        item = policy_item(policy(version=version))
        item["launch_pins_sha256"] = {"S": launch_pins_sha256(self.pins if pins is None else pins)}
        return item

    def read(self, row, pins=None):
        api = Mock()
        api.get_item.return_value = {"Item": row}
        return handler.read_policy(api, self.cloud.config.table, "483465", launch_pins=pins)

    def test_digest_is_canonical_and_domain_separated_from_a_bare_pin_hash(self):
        import hashlib
        self.assertEqual(launch_pins_sha256(dict(reversed(list(self.pins.items())))),
                         launch_pins_sha256(self.pins))
        self.assertNotEqual(launch_pins_sha256(self.pins), hashlib.sha256(canonical(self.pins)).hexdigest())
        for key, value in (("ami", "ami-aaaaaaaaaaaaaaaaa"), ("lt_version", "987"),
                           ("source", "f" * 40), ("subnet", "subnet-aaaaaaaaaaaaaaaaa")):
            with self.subTest(key=key):
                self.assertNotEqual(launch_pins_sha256(self.pins), launch_pins_sha256(self.pins | {key: value}))

    def test_digest_rejects_incomplete_extra_or_malformed_pins(self):
        for pins in (None, {}, self.pins | {"unexpected": "ignored"},
                     self.pins | {"ami": "caller-selected"}, self.pins | {"lt_version": 2}):
            with self.subTest(kind=type(pins)), self.assertRaises(ValueError):
                launch_pins_sha256(pins)

    def test_matching_bound_envelope_preserves_authority(self):
        self.assertEqual(self.read(self.row(), self.pins), policy(version="deployment-new"))

    def test_binding_is_optional_for_all_existing_legacy_payload_shapes(self):
        full = policy_item()
        value = json.loads(full["payload"]["S"])
        shapes = (
            handler._LEGACY_POLICY_FIELDS,
            handler._FILE_SET_POLICY_FIELDS,
            handler._EXTENDED_RUNTIME_POLICY_FIELDS,
            handler._POLICY_FIELDS,
        )
        for fields in shapes:
            row = copy.deepcopy(full)
            row["payload"]["S"] = canonical({key: value[key] for key in fields}).decode()
            with self.subTest(fields=len(fields)):
                self.assertEqual(self.read(row), policy())
                row["launch_pins_sha256"] = {"S": launch_pins_sha256(self.pins)}
                self.assertEqual(self.read(row, self.pins), policy())

    def test_bound_envelope_requires_matching_explicit_configuration(self):
        for pins in (None, self.pins | {"ami": "ami-aaaaaaaaaaaaaaaaa"}, self.pins | {"source": "f" * 40}):
            with self.subTest(pins=pins), self.assertRaisesRegex(ValueError, "policy unavailable"):
                self.read(self.row(), pins)

    def test_binding_types_length_case_and_unknown_envelope_fields_fail_closed(self):
        for binding in (None, "f" * 64, {"N": "1"}, {"S": ""}, {"S": "F" * 64},
                        {"S": "f" * 63}, {"S": "f" * 64, "N": "1"}):
            with self.subTest(binding=binding), self.assertRaisesRegex(ValueError, "policy unavailable"):
                row = self.row()
                row["launch_pins_sha256"] = binding
                self.read(row, self.pins)
        with self.assertRaises(ValueError):
            self.read(self.row() | {"extra": {"S": "ignored"}}, self.pins)

    def test_policy_binding_does_not_log_received_digest_or_pins(self):
        row = self.row()
        row["launch_pins_sha256"] = {"S": "f" * 64}
        with self.assertRaisesRegex(ValueError, "policy unavailable") as caught:
            self.read(row, self.pins)
        self.assertNotIn("f" * 64, str(caught.exception))
        self.assertNotIn(self.pins["ami"], str(caught.exception))

    def service(self, config):
        cloud = self.cloud
        with patch.dict("os.environ", environment(cloud) | {
                "RCP_DURABLE_" + key.upper(): value for key, value in vars(config).items()}, clear=True), \
                patch("boto3.client", side_effect=AssertionError("no SDK clients")):
            service = handler.build_admission(api=cloud.api, clock=lambda: NOW.timestamp())
        service.verifier = cloud.verifier
        service.run_reader = cloud.reader
        return service

    def test_old_invocation_first_reading_new_policy_cannot_admit_old_pins(self):
        # Capture the old environment before the simulated cutover. Its policy
        # reader runs only later, after the new bound enabled row is installed.
        old_service = self.service(self.cloud.config)
        updated = replace(self.cloud.config, ami="ami-aaaaaaaaaaaaaaaaa", lt_version="987", source="f" * 40)
        self.cloud.ddb.items["policy"] = self.row(pins=updated.pins())
        counters = copy.deepcopy(self.cloud.ddb.items)
        with self.assertRaisesRegex(ValueError, "policy unavailable"):
            old_service.begin("test-only-token", inputs())
        self.assertEqual(self.cloud.ddb.items, counters)
        self.assertEqual(self.cloud.ddb.writes, [])
        self.cloud.reader.read.assert_not_called()
        self.cloud.api["s3"].generate_presigned_url.assert_not_called()
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()
        result = self.service(updated).begin("test-only-token", inputs())
        self.assertEqual(result["state"], "awaiting-inputs")
        record = self.cloud.service.store(self.cloud.who).read()
        self.assertEqual(record.state["request"]["pins"], updated.pins())

    def test_prepared_old_decision_loses_transaction_after_new_policy_epoch(self):
        service = self.service(self.cloud.config)
        self.cloud.ddb.items["policy"] = self.row(version="deployment-before")
        original = copy.deepcopy(self.cloud.ddb.items)
        # Interrupt immediately before the real RunStore transaction, after the
        # old policy and launch request have already been captured.
        self.cloud.ddb.before_write = lambda: self.cloud.ddb.items.update(
            policy=self.row(version="deployment-after"))
        with self.assertRaises(ReconciliationRequired):
            service.begin("test-only-token", inputs())
        self.assertNotIn("active", self.cloud.ddb.items)
        self.assertEqual(self.cloud.ddb.items["budget#total"], original["budget#total"])
        self.cloud.api["s3"].generate_presigned_url.assert_not_called()
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_bound_envelope_keeps_the_watchdog_kill_switch_effective(self):
        from .test_production_runtime import ProductionCloud
        cloud = ProductionCloud()
        row = policy_item(policy(enabled=False, version="deployment-closed"))
        row["launch_pins_sha256"] = {"S": launch_pins_sha256(cloud.config.pins())}
        cloud.database.items["policy"] = row
        state = cloud.state
        cloud.service.watchdog_state(state, [])
        self.assertEqual(state["phase"], "closing")
        self.assertEqual(state["close_reason"], "production kill switch is off")

    def test_new_environment_cannot_use_rollback_policy_bound_to_old_pins(self):
        updated = replace(self.cloud.config, ami="ami-aaaaaaaaaaaaaaaaa", lt_version="987")
        self.cloud.ddb.items["policy"] = self.row(pins=self.pins, version="deployment-rollback")
        with self.assertRaisesRegex(ValueError, "policy unavailable"):
            self.service(updated).begin("test-only-token", inputs())
        self.assertEqual(self.cloud.ddb.writes, [])
        self.assertEqual(self.service(self.cloud.config).begin("test-only-token", inputs())["state"],
                         "awaiting-inputs")


if __name__ == "__main__":
    unittest.main()
