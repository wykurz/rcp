"""Offline transaction, race and ambiguous-response tests for retained run ownership."""

import copy
from dataclasses import replace
import threading
import unittest
from unittest.mock import Mock, patch

from infra.aws_benchmark.production_store import (AdmissionDecision, MAX_INTEGER, MAX_STATE_BYTES,
                                                 ReconciliationRequired, RunStore)


class ConditionalFailure(Exception):
    pass


class DynamoTransactions:
    """Evaluate only this store's expressions against serial, atomic snapshots.

    This is not an AWS emulator. SDK shape tests separately check the actual wire
    payload; these tests exercise adversarial interleavings using its conditions.
    """

    def __init__(self):
        self.items = {"policy": {"pk": {"S": "policy"}, "version": {"S": "v1"}, "enabled": {"BOOL": True}},
                      "budget#total": {"pk": {"S": "budget#total"}, "reserved": {"N": "64000000"}}}
        self.lock = threading.Lock()
        self.writes, self.reads = [], []
        self.before_write = None
        self.after_commit = None
        self.read_error = None

    def transact_get_items(self, **parameters):
        self.reads.append(copy.deepcopy(parameters))
        if self.read_error:
            raise self.read_error
        with self.lock:
            return {"Responses": [{"Item": copy.deepcopy(self.items.get(v["Get"]["Key"]["pk"]["S"], {}))}
                                  for v in parameters["TransactItems"]]}

    def get_item(self, **parameters):
        if not parameters.get("ConsistentRead"):
            raise AssertionError("run records require consistent reads")
        if self.read_error:
            raise self.read_error
        with self.lock:
            return {"Item": copy.deepcopy(self.items.get(parameters["Key"]["pk"]["S"]))}

    def condition(self, operation, item):
        expression = operation["ConditionExpression"]
        values = operation.get("ExpressionAttributeValues", {})
        names = operation.get("ExpressionAttributeNames", {})
        if expression == "attribute_not_exists(pk)":
            return not item
        if expression in ("attribute_not_exists(pk) OR (#counter BETWEEN :zero AND :remaining)",
                          "#counter BETWEEN :zero AND :remaining"):
            if not item:
                return expression.startswith("attribute_not_exists")
            value = item.get(names["#counter"], {})
            return (set(value) == {"N"} and
                    int(values[":zero"]["N"]) <= int(value["N"]) <= int(values[":remaining"]["N"]))
        parts = expression.split(" AND ")
        if any(" = " not in part for part in parts):
            raise AssertionError("test evaluator has no semantics for " + expression)
        return all(item.get(names[field]) == values[value] for field, value in (p.split(" = ") for p in parts))

    def transact_write_items(self, **parameters):
        self.writes.append(copy.deepcopy(parameters))
        if self.before_write:
            self.before_write()
        with self.lock:
            operations = parameters["TransactItems"]
            keys = [(list(value.values())[0].get("Item") or list(value.values())[0].get("Key"))["pk"]["S"]
                    for value in operations]
            if len(set(keys)) != len(keys):
                raise AssertionError("DynamoDB forbids multiple actions on the same item")
            for value, key in zip(operations, keys):
                operation = list(value.values())[0]
                if not self.condition(operation, self.items.get(key, {})):
                    raise ConditionalFailure("condition rejected")
            changed = copy.deepcopy(self.items)
            for value, key in zip(operations, keys):
                kind, operation = next(iter(value.items()))
                if kind == "Put":
                    changed[key] = copy.deepcopy(operation["Item"])
                elif kind == "Delete":
                    del changed[key]
                elif kind == "Update":
                    item = changed.setdefault(key, copy.deepcopy(operation["Key"]))
                    values, names = operation["ExpressionAttributeValues"], operation["ExpressionAttributeNames"]
                    expression = operation["UpdateExpression"]
                    if expression.startswith("ADD "):
                        field, amount = expression[4:].split()
                        item[names[field]] = {"N": str(int(item.get(names[field], {"N": "0"})["N"]) +
                                                     int(values[amount]["N"]))}
                    elif expression.startswith("SET "):
                        for assignment in expression[4:].split(", "):
                            field, value = assignment.split(" = ")
                            item[names[field]] = copy.deepcopy(values[value])
                    else:
                        raise AssertionError("unsupported test update " + expression)
            self.items = changed
        if self.after_commit:
            self.after_commit()
        return {}


def decision(**changes):
    value = AdmissionDecision(run_key="repo-123-run-456", epoch="a" * 64, fingerprint="b" * 64,
        policy_version="v1", reserve_microusd=15_000_000, total_cap_microusd=350_000_000,
        monthly_cap_microusd=80_000_000, daily_run_limit=2, month="2026-10", day="2026-10-22",
        initial_payload={"producer": {"run_id": "456", "run_attempt": "1"},
                         "state": {"request": {"run": "456-1", "pins": {"image": "fixed"}}, "phase": "armed"}})
    return replace(value, **changes)


def store(cloud, value=None):
    value = value or decision()
    return RunStore(cloud, "production-table", value.run_key, value.epoch)


def close_run(owner):
    state, revision = owner.load()
    state["phase"] = "complete"
    revision = owner.save(state, revision)
    return owner.retire("d" * 64, revision)


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.cloud = DynamoTransactions()
        self.decision = decision()
        self.store = store(self.cloud)

    def test_admission_atomically_reserves_all_counters_and_owner(self):
        record = self.store.admit(self.decision)
        self.assertEqual(record.revision, 0)
        self.assertFalse(record.closed)
        self.assertEqual(len(self.cloud.writes), 1)
        self.assertEqual(self.cloud.items["budget#total"]["reserved"], {"N": "79000000"})
        self.assertEqual(self.cloud.items["budget#month#2026-10"]["reserved"], {"N": "15000000"})
        self.assertEqual(self.cloud.items["rate#day#2026-10-22"]["runs"], {"N": "1"})
        self.assertEqual(self.cloud.items["active"]["run_key"], {"S": self.decision.run_key})
        self.assertEqual(record.admission["initial_payload"], self.decision.initial_payload)

    def test_disabled_or_changed_policy_rejects_without_partial_charge(self):
        for name, value in (("enabled", {"BOOL": False}), ("version", {"S": "v2"})):
            with self.subTest(field=name):
                cloud = DynamoTransactions()
                cloud.items["policy"][name] = value
                before = copy.deepcopy(cloud.items)
                with self.assertRaises(ReconciliationRequired):
                    store(cloud).admit(decision())
                self.assertEqual(cloud.items, before)

    def test_exact_budget_boundary_succeeds_and_one_unit_over_fails(self):
        for field, key, limit in (("reserved", "budget#total", self.decision.total_cap_microusd),
                                  ("reserved", "budget#month#2026-10", self.decision.monthly_cap_microusd)):
            for excess in (0, 1):
                with self.subTest(key=key, excess=excess):
                    cloud = DynamoTransactions()
                    initial = limit - self.decision.reserve_microusd + excess
                    cloud.items[key] = {"pk": {"S": key}, field: {"N": str(initial)}}
                    before = copy.deepcopy(cloud.items)
                    if excess:
                        with self.assertRaises(ReconciliationRequired):
                            store(cloud).admit(self.decision)
                        self.assertEqual(cloud.items, before)
                    else:
                        store(cloud).admit(self.decision)
                        self.assertEqual(cloud.items[key][field], {"N": str(limit)})

    def test_absent_cumulative_ledger_blocks_admission_instead_of_refunding_history(self):
        del self.cloud.items["budget#total"]
        before = copy.deepcopy(self.cloud.items)
        with self.assertRaises(ReconciliationRequired):
            self.store.admit(self.decision)
        self.assertEqual(self.cloud.items, before)
        self.assertNotIn("active", self.cloud.items)

    def test_daily_limit_rejects_even_with_spare_money(self):
        self.cloud.items["rate#day#2026-10-22"] = {"pk": {"S": "rate#day#2026-10-22"}, "runs": {"N": "2"}}
        before = copy.deepcopy(self.cloud.items)
        with self.assertRaises(ReconciliationRequired):
            self.store.admit(self.decision)
        self.assertEqual(self.cloud.items, before)

    def test_missing_or_negative_existing_counter_is_not_reset(self):
        for extra in ({}, {"reserved": {"N": "-1"}}, {"reserved": {"S": "0"}}):
            with self.subTest(extra=extra):
                cloud = DynamoTransactions()
                cloud.items["budget#total"] = {"pk": {"S": "budget#total"}, **extra}
                before = copy.deepcopy(cloud.items)
                with self.assertRaises(ReconciliationRequired):
                    store(cloud).admit(decision())
                self.assertEqual(cloud.items, before)

    def test_two_concurrent_admissions_cannot_both_acquire_slot(self):
        barrier = threading.Barrier(2)
        self.cloud.before_write = lambda: barrier.wait(timeout=10)
        other = decision(run_key="repo-123-run-457", epoch="c" * 64)
        outcomes = []
        def admit(value):
            try:
                outcomes.append(store(self.cloud, value).admit(value))
            except ReconciliationRequired as error:
                outcomes.append(error)
        threads = [threading.Thread(target=admit, args=(value,)) for value in (self.decision, other)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(sum(not isinstance(v, Exception) for v in outcomes), 1)
        self.assertEqual(self.cloud.items["budget#total"]["reserved"], {"N": "79000000"})

    def test_duplicate_and_rerun_cannot_buy_another_allocation(self):
        first = self.store.admit(self.decision)
        for _ in range(3):
            self.assertEqual(self.store.admit(self.decision), first)
        self.assertEqual(len(self.cloud.writes), 1)
        changed = copy.deepcopy(self.decision.initial_payload)
        changed["producer"]["run_attempt"] = "2"
        with self.assertRaises(ReconciliationRequired):
            self.store.admit(replace(self.decision, initial_payload=changed))
        self.assertEqual(len(self.cloud.writes), 1)

    def test_same_fingerprint_cannot_hide_changed_initial_request(self):
        self.store.admit(self.decision)
        payload = copy.deepcopy(self.decision.initial_payload)
        payload["state"]["request"]["pins"]["image"] = "changed"
        with self.assertRaises(ReconciliationRequired):
            self.store.admit(replace(self.decision, initial_payload=payload))
        self.assertEqual(len(self.cloud.writes), 1)

    def test_schedule_slot_is_not_reused_after_retirement(self):
        value = replace(self.decision, schedule_slot="2026-10-22T20:00:00Z")
        self.store.admit(value)
        close_run(self.store)
        other = replace(value, run_key="repo-123-run-457", epoch="c" * 64)
        before = copy.deepcopy(self.cloud.items)
        with self.assertRaises(ReconciliationRequired):
            store(self.cloud, other).admit(other)
        self.assertEqual(self.cloud.items, before)
        self.assertIn("schedule#2026-10-22T20:00:00Z", self.cloud.items)

    def test_new_month_keeps_prior_cumulative_exposure(self):
        value = replace(self.decision, total_cap_microusd=84_000_000)
        self.store.admit(value)
        close_run(self.store)
        november = replace(value, run_key="repo-123-run-457", epoch="c" * 64,
                           month="2026-11", day="2026-11-01")
        with self.assertRaises(ReconciliationRequired):
            store(self.cloud, november).admit(november)
        self.assertEqual(self.cloud.items["budget#total"]["reserved"], {"N": "79000000"})
        self.assertNotIn("budget#month#2026-11", self.cloud.items)

    def test_timeout_after_commit_reconciles_one_charge(self):
        self.cloud.after_commit = Mock(side_effect=TimeoutError("response lost"))
        result = self.store.admit(self.decision)
        self.assertEqual(result.revision, 0)
        self.assertEqual(self.cloud.items["budget#total"]["reserved"], {"N": "79000000"})
        self.store.admit(self.decision)
        self.assertEqual(len(self.cloud.writes), 1)

    def test_timeout_before_commit_is_uncertain_and_never_retried_internally(self):
        self.cloud.before_write = Mock(side_effect=TimeoutError("outcome unknown"))
        with self.assertRaisesRegex(ReconciliationRequired, "no committed matching run"):
            self.store.admit(self.decision)
        self.assertEqual(len(self.cloud.writes), 1)
        self.assertEqual(set(self.cloud.items), {"policy", "budget#total"})

    def test_unreadable_committed_outcome_retains_hold(self):
        self.cloud.after_commit = lambda: setattr(self.cloud, "read_error", TimeoutError("read failed"))
        with self.assertRaisesRegex(ReconciliationRequired, "committed"):
            self.store.admit(self.decision)
        self.assertIn("active", self.cloud.items)
        self.assertEqual(len(self.cloud.writes), 1)

    def test_accounting_or_owner_corruption_blocks_deduplication_success(self):
        self.store.admit(self.decision)
        for key in ("budget#total", "budget#month#2026-10", "rate#day#2026-10-22", "active"):
            with self.subTest(key=key):
                item = self.cloud.items.pop(key)
                with self.assertRaises(ReconciliationRequired):
                    self.store.admit(self.decision)
                self.cloud.items[key] = item
        self.assertEqual(len(self.cloud.writes), 1)

    def test_old_closed_run_returns_tombstone_without_touching_new_owner(self):
        self.store.admit(self.decision)
        closed = close_run(self.store)
        other = decision(run_key="repo-123-run-457", epoch="c" * 64)
        store(self.cloud, other).admit(other)
        before = copy.deepcopy(self.cloud.items)
        self.assertEqual(self.store.admit(self.decision), closed)
        self.assertEqual(self.cloud.items, before)
        self.assertEqual(self.cloud.items["budget#total"]["reserved"], {"N": "94000000"})


class StateFenceTests(unittest.TestCase):
    def setUp(self):
        self.cloud = DynamoTransactions()
        self.store = store(self.cloud)
        self.store.admit(decision())

    def test_state_save_preserves_immutable_admission_and_increments_revision(self):
        before = self.store.read().admission
        state, revision = self.store.load()
        state["phase"] = "launching"
        self.assertEqual(self.store.save_launch_intent(state, revision), 1)
        after = self.store.read()
        self.assertEqual(after.admission, before)
        self.assertEqual(after.state["phase"], "launching")

    def test_launch_intent_requires_same_enabled_policy_at_transaction_time(self):
        for key, value in (("enabled", {"BOOL": False}), ("version", {"S": "v2"})):
            with self.subTest(key=key):
                cloud = DynamoTransactions()
                owner = store(cloud)
                owner.admit(decision())
                state, revision = owner.load()
                state["phase"] = "launching"
                cloud.before_write = lambda: cloud.items["policy"].update({key: value})
                with self.assertRaises(ReconciliationRequired):
                    owner.save_launch_intent(state, revision)
                self.assertEqual(owner.read().state["phase"], "armed")
                self.assertEqual(owner.read().revision, revision)

    def test_cleanup_progress_remains_writable_after_policy_disabled(self):
        self.cloud.items["policy"]["enabled"] = {"BOOL": False}
        state, revision = self.store.load()
        state["phase"] = "closing"
        self.assertEqual(self.store.save(state, revision), revision + 1)
        state["phase"] = "complete"
        self.assertEqual(self.store.save(state, revision + 1), revision + 2)
        self.assertTrue(self.store.retire("d" * 64, revision + 2).closed)

    def test_normal_save_cannot_bypass_policy_fence_for_launch(self):
        state, revision = self.store.load()
        state["phase"] = "launching"
        with self.assertRaisesRegex(ValueError, "policy-fenced"):
            self.store.save(state, revision)
        self.assertEqual(self.store.read().revision, revision)

    def test_launch_intent_cannot_rearm_an_uncertain_or_closing_run(self):
        for phase in ("uncertain", "closing", "requested"):
            with self.subTest(phase=phase):
                cloud = DynamoTransactions()
                owner = store(cloud)
                owner.admit(decision())
                state, revision = owner.load()
                state["phase"] = phase
                revision = owner.save(state, revision)
                state["phase"] = "launching"
                with self.assertRaisesRegex(ValueError, "armed run"):
                    owner.save_launch_intent(state, revision)

    def test_terminal_state_cannot_be_reopened_by_late_launch_callback(self):
        state, revision = self.store.load()
        state["phase"] = "complete"
        self.store.save(state, revision)
        state["phase"] = "requested"
        with self.assertRaisesRegex(ValueError, "stale or already terminal"):
            self.store.save(state, revision + 1)
        self.assertEqual(self.store.read().state["phase"], "complete")

    def test_changed_request_or_stale_revision_cannot_write(self):
        state, revision = self.store.load()
        changed = copy.deepcopy(state)
        changed["request"]["pins"]["image"] = "another"
        with self.assertRaisesRegex(ValueError, "immutable request"):
            self.store.save(changed, revision)
        self.store.save(state, revision)
        with self.assertRaisesRegex(ValueError, "stale"):
            self.store.save(state, revision)

    def test_owner_changes_after_read_are_caught_by_transaction(self):
        state, revision = self.store.load()
        self.cloud.before_write = lambda: self.cloud.items["active"].update(epoch={"S": "f" * 64})
        state["phase"] = "launching"
        with self.assertRaises(ReconciliationRequired):
            self.store.save_launch_intent(state, revision)
        self.assertEqual(self.store.read().state["phase"], "armed")

    def test_two_state_writers_cannot_overwrite_the_same_revision(self):
        barrier = threading.Barrier(2)
        self.cloud.before_write = lambda: barrier.wait(timeout=10)
        outcomes = []
        def save(phase):
            try:
                state, revision = self.store.load()
                state["phase"] = phase
                outcomes.append(self.store.save_launch_intent(state, revision) if phase == "launching" else
                                self.store.save(state, revision))
            except ReconciliationRequired as error:
                outcomes.append(error)
        threads = [threading.Thread(target=save, args=(phase,)) for phase in ("launching", "closing")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(sum(type(v) is int for v in outcomes), 1)
        self.assertEqual(self.store.read().revision, 1)

    def test_timeout_after_state_commit_returns_only_exact_matching_revision(self):
        self.cloud.after_commit = Mock(side_effect=TimeoutError("lost response"))
        state, revision = self.store.load()
        state["phase"] = "launching"
        self.assertEqual(self.store.save_launch_intent(state, revision), 1)
        self.assertEqual(self.store.read().state, state)

    def test_retirement_requires_terminal_state_and_leaves_permanent_evidence(self):
        with self.assertRaisesRegex(ValueError, "terminal owner revision"):
            self.store.retire("d" * 64, 0)
        before = copy.deepcopy(self.cloud.items["budget#total"])
        closed = close_run(self.store)
        self.assertTrue(closed.closed)
        self.assertEqual(closed.retirement_sha256, "d" * 64)
        self.assertNotIn("active", self.cloud.items)
        self.assertEqual(self.cloud.items["budget#total"], before)
        self.assertFalse(any("ttl" in item or "expires_at" in item for item in self.cloud.items.values()))
        with self.assertRaisesRegex(ValueError, "active slot"):
            self.store.save(closed.state, closed.revision)

    def test_retirement_replay_returns_same_tombstone_but_different_proof_fails(self):
        closed = close_run(self.store)
        self.assertEqual(self.store.retire("d" * 64, closed.revision - 1), closed)
        with self.assertRaisesRegex(ValueError, "retained tombstone"):
            self.store.retire("e" * 64, closed.revision - 1)

    def test_retirement_timeout_after_commit_is_reconciled_without_new_delete(self):
        state, revision = self.store.load()
        state["phase"] = "complete"
        revision = self.store.save(state, revision)
        self.cloud.after_commit = Mock(side_effect=TimeoutError("lost response"))
        result = self.store.retire("d" * 64, revision)
        self.assertTrue(result.closed)
        writes = len(self.cloud.writes)
        self.store.retire("d" * 64, revision)
        self.assertEqual(len(self.cloud.writes), writes)

    def test_retirement_lost_response_and_unreadable_result_keeps_tombstone(self):
        state, revision = self.store.load()
        state["phase"] = "complete"
        revision = self.store.save(state, revision)
        self.cloud.after_commit = lambda: setattr(self.cloud, "read_error", TimeoutError("read failed"))
        with self.assertRaisesRegex(ReconciliationRequired, "committed"):
            self.store.retire("d" * 64, revision)
        self.assertTrue(self.cloud.items[self.store.pk]["closed"]["BOOL"])
        self.assertNotIn("active", self.cloud.items)

    def test_retirement_cannot_delete_another_epoch(self):
        state, revision = self.store.load()
        state["phase"] = "complete"
        revision = self.store.save(state, revision)
        self.cloud.before_write = lambda: self.cloud.items["active"].update(epoch={"S": "e" * 64})
        with self.assertRaises(ReconciliationRequired):
            self.store.retire("d" * 64, revision)
        self.assertFalse(self.store.read().closed)
        self.assertEqual(self.cloud.items["active"]["epoch"], {"S": "e" * 64})

    def test_wrong_epoch_cannot_load_or_reconcile_an_old_run(self):
        wrong = RunStore(self.cloud, "production-table", self.store.run_key, "c" * 64)
        with self.assertRaisesRegex(ValueError, "owner differs"):
            wrong.load()
        with self.assertRaises(ReconciliationRequired):
            wrong.admit(replace(decision(), epoch="c" * 64))


class ValidationTests(unittest.TestCase):
    def test_invalid_decisions_are_rejected_before_any_io(self):
        for kwargs in ({"reserve_microusd": True}, {"reserve_microusd": 0}, {"reserve_microusd": MAX_INTEGER + 1},
                       {"monthly_cap_microusd": 1}, {"daily_run_limit": 0}, {"day": "2026-02-30", "month": "2026-02"},
                       {"month": "2026-11"}, {"run_key": "repo-123-run-456-attempt-2"},
                       {"initial_payload": {"state": None}}, {"epoch": "not-a-digest"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                decision(**kwargs)

    def test_mutated_decision_dictionary_is_revalidated_before_admission(self):
        value = decision()
        value.initial_payload["state"]["phase"] = "complete"
        cloud = DynamoTransactions()
        with self.assertRaises(ValueError):
            store(cloud).admit(value)
        self.assertEqual(cloud.writes, [])

    def test_identical_logical_writes_do_not_reuse_a_cached_success_token(self):
        cloud = Mock()
        owner = store(cloud)
        items = owner._admission_items(decision())
        owner._write(items)
        owner._write(items)
        first, second = (call.kwargs for call in cloud.transact_write_items.call_args_list)
        self.assertEqual(first["TransactItems"], second["TransactItems"])
        self.assertNotEqual(first["ClientRequestToken"], second["ClientRequestToken"])

    def test_nonfinite_oversized_and_invalid_state_fail_before_write(self):
        cloud = DynamoTransactions()
        owner = store(cloud)
        owner.admit(decision())
        state, revision = owner.load()
        for value in (float("nan"), "x" * MAX_STATE_BYTES):
            with self.subTest(value_type=type(value).__name__):
                changed = dict(state, diagnostic=value)
                with self.assertRaises(ValueError):
                    owner.save(changed, revision)
        self.assertEqual(owner.read().revision, 0)
        self.assertEqual(len(cloud.writes), 1)

    def test_complete_item_bound_includes_immutable_admission_and_new_state(self):
        cloud = DynamoTransactions()
        owner = store(cloud)
        payload = decision().initial_payload
        payload["producer"]["diagnostic"] = "x" * 350_000
        with patch("infra.aws_benchmark.production_store.MAX_ADMISSION_BYTES", 400 * 1024):
            owner.admit(decision(initial_payload=payload))
            state, revision = owner.load()
            state["diagnostic"] = "y" * 100_000
            with self.assertRaisesRegex(ValueError, "complete DynamoDB item"):
                owner.save(state, revision)
            self.assertEqual(owner.read().revision, revision)
        self.assertEqual(len(cloud.writes), 1)

    def test_production_state_accepts_maximum_receipt_scale_without_affecting_legacy(self):
        cloud = DynamoTransactions()
        owner = store(cloud)
        owner.admit(decision())
        state, revision = owner.load()
        state["diagnostic"] = "x" * 103_000
        self.assertEqual(owner.save(state, revision), revision + 1)
        from infra.aws_benchmark.durable_aws import MAX_RECEIPT
        self.assertEqual(MAX_RECEIPT, 100_000)

    def test_generated_requests_match_local_sdk_shapes(self):
        import botocore.session
        from botocore.validate import validate_parameters
        cloud = DynamoTransactions()
        owner = store(cloud)
        owner.admit(decision(schedule_slot="2026-10-22T20:00:00Z"))
        state, revision = owner.load()
        state["phase"] = "launching"
        owner.save_launch_intent(state, revision)
        close_run(owner)
        model = botocore.session.get_session().get_service_model("dynamodb")
        for operation, values in (("TransactWriteItems", cloud.writes), ("TransactGetItems", cloud.reads)):
            shape = model.operation_model(operation).input_shape
            for parameters in values:
                validate_parameters(parameters, shape)
        for parameters in cloud.writes:
            self.assertLessEqual(len(parameters["ClientRequestToken"]), 36)
            deletes = [v["Delete"]["Key"]["pk"]["S"] for v in parameters["TransactItems"] if "Delete" in v]
            self.assertTrue(all(key == "active" for key in deletes))


if __name__ == "__main__":
    unittest.main()
