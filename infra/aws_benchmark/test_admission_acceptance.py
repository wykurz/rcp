"""Offline candidate checks; no AWS client factory, credentials or network."""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

from botocore.exceptions import ClientError
from botocore.session import get_session
from botocore.validate import validate_parameters

from . import admission_acceptance as candidate
from infra.aws_benchmark.test_production_store import DynamoTransactions, ConditionalFailure
from infra.aws_benchmark.production_store import ReconciliationRequired

NOW = datetime(2026, 11, 3, 23, 0, tzinfo=timezone.utc)
TABLE = "rcp-production-admission-acceptance-0123456789ab"


class ShapedTransactions(DynamoTransactions):
    """Existing expression model plus real pinned botocore input-shape validation.

    This is not DynamoDB and does not establish live API/IAM acceptance.
    """
    def __init__(self, phase):
        super().__init__()
        self.items = copy.deepcopy(candidate.fixture_items(phase, NOW))
        self.model = get_session().get_service_model("dynamodb")
        self.forwarded = []

    def validate(self, method, request):
        operation = {"get_item": "GetItem", "transact_get_items": "TransactGetItems",
            "transact_write_items": "TransactWriteItems"}[method]
        validate_parameters(request, self.model.operation_model(operation).input_shape)
        self.forwarded.append(method)

    def get_item(self, **request):
        self.validate("get_item", request)
        return super().get_item(**request)

    def transact_get_items(self, **request):
        self.validate("transact_get_items", request)
        return super().transact_get_items(**request)

    def transact_write_items(self, **request):
        self.validate("transact_write_items", request)
        try:
            return super().transact_write_items(**request)
        except ConditionalFailure:
            reasons = []
            for action in request["TransactItems"]:
                operation = next(iter(action.values()))
                key = (operation.get("Key") or operation.get("Item"))["pk"]["S"]
                reasons.append({"Code": "None" if self.condition(operation,
                    self.items.get(key, {})) else "ConditionalCheckFailed"})
            raise ClientError({"Error": {"Code": "TransactionCanceledException"},
                "CancellationReasons": reasons}, "TransactWriteItems") from None


class CandidateTests(unittest.TestCase):
    def test_all_prepared_phases_use_unchanged_real_service_and_wire_shapes(self):
        for phase in candidate.PHASES:
            with self.subTest(phase=phase):
                model = ShapedTransactions(phase)
                result = candidate.run_phase(model, TABLE, phase, NOW)
                self.assertTrue(result["ok"])
                self.assertFalse(result["live_accepted"])
                self.assertTrue(result["synthetic_identity"])
                self.assertTrue(result["synthetic_github_proof"])
                self.assertTrue(result["no_seal_or_start"])
                self.assertLessEqual(result["calls"], candidate.CALL_LIMIT)
                self.assertEqual(result["calls"], len(model.forwarded))

    def test_counters_fail_independently_without_any_active_owner(self):
        for phase, key in (("total-cap", "budget#total"),
                           ("monthly-cap", "budget#month#2026-11"),
                           ("daily-cap", "rate#day#2026-11-03")):
            with self.subTest(phase=phase):
                model = ShapedTransactions(phase)
                self.assertNotIn("active", model.items)
                seed = copy.deepcopy(model.items)
                candidate.run_phase(model, TABLE, phase, NOW)
                self.assertEqual(model.items, seed)
                self.assertIn(key, seed)
                self.assertEqual(len(model.writes), 1)

    def test_exact_cap_controls_admit_where_one_micro_or_one_run_over_rejects(self):
        for phase, key, field, cap in (
                ("total-exact", "budget#total", "reserved", 350_000_000),
                ("monthly-exact", "budget#month#2026-11", "reserved", 80_000_000),
                ("daily-exact", "rate#day#2026-11-03", "runs", candidate.MAX_WORKERS)):
            with self.subTest(phase=phase):
                model = ShapedTransactions(phase)
                candidate.run_phase(model, TABLE, phase, NOW)
                self.assertEqual(model.items[key][field], {"N": str(cap)})

    def test_race_detects_a_broken_owner_guard_without_daily_or_budget_masking(self):
        model = ShapedTransactions("competing-runs")
        original = model.condition
        def broken(operation, item):
            key = (operation.get("Key") or operation.get("Item"))["pk"]["S"]
            return True if key == "active" else original(operation, item)
        model.condition = broken
        with self.assertRaisesRegex(ValueError, "exactly one competing"):
            candidate.run_phase(model, TABLE, "competing-runs", NOW)
        self.assertEqual(len([key for key in model.items if key.startswith("run#")]), 4)
        self.assertEqual(model.items["rate#day#2026-11-03"]["runs"], {"N": "4"})

    def test_race_corrupt_committed_winner_is_not_accepted_from_partial_manual_snapshot(self):
        model = ShapedTransactions("competing-runs")
        def corrupt_winner():
            for key, item in model.items.items():
                if key.startswith("run#"):
                    item["fingerprint"] = {"S": "f" * 64}
        model.after_commit = corrupt_winner
        with self.assertRaisesRegex(ValueError, "fully reconcile"):
            candidate.run_phase(model, TABLE, "competing-runs", NOW)

    def test_realistic_transaction_conflicts_cannot_mask_a_missing_owner_fence(self):
        model = ShapedTransactions("competing-runs")
        original_condition, original_write = model.condition, model.transact_write_items
        def broken(operation, item):
            key = (operation.get("Key") or operation.get("Item"))["pk"]["S"]
            return True if key == "active" else original_condition(operation, item)
        model.condition = broken
        count = 0
        lock = __import__("threading").Lock()
        def conflicts(**request):
            nonlocal count
            with lock:
                count += 1
                selected = count
            if 2 <= selected <= 4:
                raise ClientError({"Error": {"Code": "TransactionCanceledException"},
                    "CancellationReasons": [{"Code": "TransactionConflict"}] +
                        [{"Code": "None"}] * 5}, "TransactWriteItems")
            return original_write(**request)
        model.transact_write_items = conflicts
        with self.assertRaisesRegex(AssertionError, "expected admission rejection"):
            candidate.run_phase(model, TABLE, "competing-runs", NOW)
        self.assertEqual(count, 5)

    def test_legitimate_conflict_losers_still_require_a_serial_owner_rejection(self):
        model = ShapedTransactions("competing-runs")
        original_write = model.transact_write_items
        count = 0
        lock = __import__("threading").Lock()
        def conflicts(**request):
            nonlocal count
            with lock:
                count += 1
                selected = count
            if 2 <= selected <= 4:
                raise ClientError({"Error": {"Code": "TransactionCanceledException"},
                    "CancellationReasons": [{"Code": "TransactionConflict"}] +
                        [{"Code": "None"}] * 5}, "TransactWriteItems")
            return original_write(**request)
        model.transact_write_items = conflicts
        result = candidate.run_phase(model, TABLE, "competing-runs", NOW)
        self.assertTrue(result["ok"])
        self.assertEqual(count, 5)
        self.assertEqual(result["write_outcomes"][-1]["reasons"],
            ["None", "None", "ConditionalCheckFailed", "None", "None", "None"])

    def test_removed_counter_guard_cannot_pass_a_limit_case(self):
        for phase, counter in (("total-cap", "budget#total"),
                               ("monthly-cap", "budget#month#2026-11"),
                               ("daily-cap", "rate#day#2026-11-03")):
            with self.subTest(phase=phase):
                model = ShapedTransactions(phase)
                original = model.condition
                def broken(operation, item):
                    key = (operation.get("Key") or operation.get("Item"))["pk"]["S"]
                    return True if key == counter else original(operation, item)
                model.condition = broken
                with self.assertRaisesRegex(AssertionError, "expected admission rejection"):
                    candidate.run_phase(model, TABLE, phase, NOW)

    def test_fixture_rejects_a_preexisting_owner_before_any_write(self):
        model = ShapedTransactions("dedup")
        model.items["active"] = {"pk": {"S": "active"}, "run_key": {"S": "foreign"}}
        with self.assertRaisesRegex(ValueError, "seed or retained owner"):
            candidate.run_phase(model, TABLE, "dedup", NOW)
        self.assertEqual(model.writes, [])

    def test_total_missing_is_not_implicitly_zero(self):
        model = ShapedTransactions("missing-ledger")
        candidate.run_phase(model, TABLE, "missing-ledger", NOW)
        self.assertNotIn("budget#total", model.items)
        self.assertEqual(len(model.writes), 1)

    def test_policy_fence_rejection_reaches_the_transaction(self):
        model = ShapedTransactions("policy-fence")
        candidate.run_phase(model, TABLE, "policy-fence", NOW)
        self.assertEqual(len(model.writes), 1)
        condition = model.writes[0]["TransactItems"][0]["ConditionCheck"]
        self.assertEqual(condition["ExpressionAttributeValues"][":version"], {"S": "fixture-1"})
        self.assertEqual(model.items["policy"]["version"], {"S": "fixture-2"})

    def test_disabled_stale_actor_and_event_rejections_have_no_transaction(self):
        for phase in ("disabled-policy", "stale-price", "unauthorized-actor", "unapproved-event"):
            with self.subTest(phase=phase):
                model = ShapedTransactions(phase)
                candidate.run_phase(model, TABLE, phase, NOW)
                self.assertEqual(model.writes, [])

    def test_concurrent_owners_make_four_contending_transactions_one_charge(self):
        for _ in range(10):
            model = ShapedTransactions("competing-runs")
            candidate.run_phase(model, TABLE, "competing-runs", NOW)
            self.assertEqual(len(model.writes), 5)
            self.assertEqual(len([key for key in model.items if key.startswith("run#")]), 1)

    def test_lost_write_response_adopts_committed_record_without_second_charge(self):
        model = ShapedTransactions("lost-write-response")
        candidate.run_phase(model, TABLE, "lost-write-response", NOW)
        # one admission plus an independently rejected other run; dedup never re-writes
        self.assertEqual(len(model.writes), 2)
        self.assertEqual(model.items["rate#day#2026-11-03"]["runs"], {"N": "1"})

    def test_unknown_phase_or_production_table_never_touches_client(self):
        for table, phase in (("production-table", "dedup"), (TABLE, "unknown")):
            client = Mock()
            with self.assertRaises(ValueError):
                candidate.run_phase(client, table, phase, NOW)
            self.assertEqual(client.mock_calls, [])

    def test_wrong_table_key_delete_and_service_methods_are_blocked(self):
        model = ShapedTransactions("dedup")
        wrapper = candidate.FixtureDynamo(model, TABLE, NOW)
        with self.assertRaises(ValueError):
            wrapper.get_item(TableName="production-table", Key={"pk": {"S": "policy"}}, ConsistentRead=True)
        with self.assertRaises(ValueError):
            wrapper.get_item(TableName=TABLE, Key={"pk": {"S": "foreign"}}, ConsistentRead=True)
        with self.assertRaises(ValueError):
            wrapper.transact_write_items(ClientRequestToken="a"*36,
                TransactItems=[{"Delete": {"TableName": TABLE, "Key": {"pk": {"S": "active"}}}}])
        for method in ("delete_item", "put_item", "scan", "start_execution", "get_secret_value"):
            with self.assertRaises(AttributeError):
                getattr(wrapper, method)
        self.assertEqual(model.forwarded, [])

    def test_shared_call_limit_is_checked_before_forwarding(self):
        model = ShapedTransactions("dedup")
        wrapper = candidate.FixtureDynamo(model, TABLE, NOW)
        for _ in range(candidate.CALL_LIMIT):
            wrapper.get_item(TableName=TABLE, Key={"pk": {"S": "policy"}}, ConsistentRead=True)
        with self.assertRaisesRegex(ValueError, "call budget"):
            wrapper.get_item(TableName=TABLE, Key={"pk": {"S": "policy"}}, ConsistentRead=True)
        self.assertEqual(len(model.forwarded), candidate.CALL_LIMIT)

    def test_inert_uploads_and_no_launch_have_no_network_or_credential_path(self):
        model = ShapedTransactions("dedup")
        wrapper = candidate.FixtureDynamo(model, TABLE, NOW)
        service = candidate.service(wrapper, TABLE, NOW, candidate.identity(), "dedup")
        result = service.begin("SYNTHETIC-NOT-A-JWT", candidate.manifest())
        self.assertEqual(len(result["uploads"]), 6)
        self.assertTrue(all(upload["url"] == "https://no-upload.invalid/INERT-NOT-A-CAPABILITY"
            for upload in result["uploads"].values()))
        with self.assertRaises(AssertionError):
            service.api["stepfunctions"].start_execution()
        with self.assertRaises(AttributeError):
            service.api["s3"].head_object()

    def test_transient_read_and_unreconciled_write_never_pass(self):
        model = ShapedTransactions("dedup")
        model.read_error = TimeoutError("synthetic unreadable read")
        with self.assertRaises(TimeoutError):
            candidate.run_phase(model, TABLE, "dedup", NOW)

    def test_access_validation_throttle_and_wrong_condition_do_not_count_as_cap_rejection(self):
        for phase in ("total-cap", "monthly-cap", "daily-cap", "missing-ledger", "policy-fence"):
            for code in ("AccessDeniedException", "ValidationException", "ThrottlingException",
                         "TransactionCanceledException"):
                with self.subTest(phase=phase, code=code):
                    model = ShapedTransactions(phase)
                    def fail(**request):
                        raise ClientError({"Error": {"Code": code},
                            "CancellationReasons": [{"Code": "None"}] * 6}, "TransactWriteItems")
                    model.transact_write_items = fail
                    with self.assertRaises((ValueError, ReconciliationRequired, ClientError)):
                        candidate.run_phase(model, TABLE, phase, NOW)

    def test_race_access_denial_cannot_impersonate_losing_contenders(self):
        model = ShapedTransactions("competing-runs")
        original = model.transact_write_items
        count = 0
        gate = __import__("threading").Lock()
        def selected_failure(**request):
            nonlocal count
            with gate:
                count += 1
                selected = count == 1
            if selected:
                return original(**request)
            raise ClientError({"Error": {"Code": "AccessDeniedException"}}, "TransactWriteItems")
        model.transact_write_items = selected_failure
        with self.assertRaisesRegex(ValueError, "race did not prove"):
            candidate.run_phase(model, TABLE, "competing-runs", NOW)

    def test_policy_unavailable_cannot_impersonate_a_disabled_policy(self):
        model = ShapedTransactions("disabled-policy")
        original = model.get_item
        def failed(**request):
            if request["Key"]["pk"]["S"] == "policy":
                raise ClientError({"Error": {"Code": "AccessDeniedException"}}, "GetItem")
            return original(**request)
        model.get_item = failed
        with self.assertRaisesRegex(ValueError, "expected pre-write policy"):
            candidate.run_phase(model, TABLE, "disabled-policy", NOW)

    def test_ambiguous_postcommit_read_cannot_release_the_owner(self):
        model = ShapedTransactions("lost-write-response")
        def after_commit():
            model.read_error = TimeoutError("injected unreadable committed result")
        model.after_commit = after_commit
        with self.assertRaises((TimeoutError, ReconciliationRequired)):
            candidate.run_phase(model, TABLE, "lost-write-response", NOW)
        self.assertIn("active", model.items)
        self.assertEqual(model.items["rate#day#2026-11-03"]["runs"], {"N": "1"})

    def test_every_fixture_can_be_serialized_without_secret_inputs(self):
        for phase in candidate.PHASES:
            seed = candidate.fixture_items(phase, NOW)
            self.assertNotIn("active", seed)
            self.assertTrue(all(not key.startswith("run#") for key in seed))
            self.assertIn("SYNTHETIC FIXTURE", json.dumps(seed))
            self.assertEqual(json.loads(json.dumps(seed)), seed)


if __name__ == "__main__":
    unittest.main()
