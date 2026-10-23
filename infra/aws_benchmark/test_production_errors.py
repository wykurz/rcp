"""Failure classification must preserve both progress and transactional ownership."""

import copy
import unittest
from unittest.mock import Mock

from botocore.exceptions import ClientError

from infra.aws_benchmark.common import retryable_io
from infra.aws_benchmark.production_store import ReconciliationRequired
from infra.aws_benchmark.production_errors import RevisionConflict, transient
from infra.aws_benchmark.test_production_store import DynamoTransactions, decision, store


def api_error(code, *, reasons=None, status=400):
    response = {"Error": {"Code": code, "Message": "injected"},
                "ResponseMetadata": {"HTTPStatusCode": status}}
    if reasons is not None:
        response["CancellationReasons"] = reasons
    return ClientError(response, "TransactWriteItems")


def cancellation(*codes):
    return api_error("TransactionCanceledException", reasons=[{"Code": code} for code in codes])


class ErrorClassificationTests(unittest.TestCase):
    def test_only_explicit_transient_transaction_reasons_are_retryable(self):
        for codes in (("TransactionConflict",), ("None", "TransactionConflict"),
                      ("ThrottlingError", "None"), ("ProvisionedThroughputExceeded",),
                      ("TransactionConflict", "ThrottlingError", "ProvisionedThroughputExceeded")):
            with self.subTest(codes=codes):
                self.assertTrue(retryable_io(cancellation(*codes)))

    def test_mixed_permanent_missing_or_malformed_reasons_fail_closed(self):
        for reasons in (None, [], [{"Code": "None"}], [{"Code": "Unknown"}],
                        [{"Code": "TransactionConflict"}, {"Code": "ConditionalCheckFailed"}],
                        [{"Code": "ThrottlingError"}, {"Code": "ValidationError"}],
                        [{"Code": "TransactionConflict"}, {"Code": "AccessDenied"}],
                        [{"Code": "ProvisionedThroughputExceeded"}, {"Code": "ItemCollectionSizeLimitExceeded"}],
                        [{"Code": "TransactionConflict"}, {}], [None], [{"Code": None}],
                        [{"Code": ["TransactionConflict"]}], {"Code": "TransactionConflict"}):
            with self.subTest(reasons=reasons):
                # a contradictory HTTP status cannot override a known permanent reason
                self.assertFalse(retryable_io(api_error("TransactionCanceledException", reasons=reasons, status=500)))

    def test_transport_throttle_and_server_errors_remain_retryable(self):
        for error in (TimeoutError("lost response"), ConnectionResetError("reset"),
                      api_error("ThrottlingException"), api_error("TooManyRequestsException", status=429),
                      api_error("InternalServerError", status=500)):
            with self.subTest(error=error):
                self.assertTrue(retryable_io(error))

    def test_access_validation_conditional_and_arbitrary_cause_are_not_transient(self):
        for code in ("AccessDeniedException", "ValidationException", "ConditionalCheckFailedException",
                     "ResourceNotFoundException", "UnrecognizedClientException"):
            with self.subTest(code=code):
                self.assertFalse(retryable_io(api_error(code)))
        wrapped = ReconciliationRequired("invalid readback")
        wrapped.__cause__ = TimeoutError("earlier transport failure")
        self.assertFalse(retryable_io(wrapped))


class StoreFailureTests(unittest.TestCase):
    def setUp(self):
        self.cloud = DynamoTransactions()
        self.store = store(self.cloud)
        self.store.admit(decision())

    def test_no_commit_throttle_reset_and_conflict_preserve_retryability_and_revision(self):
        for error in (api_error("ThrottlingException"), ConnectionResetError("reset"),
                      cancellation("None", "TransactionConflict")):
            for launch in (False, True):
                with self.subTest(error=error, launch=launch):
                    cloud = DynamoTransactions()
                    owner = store(cloud)
                    owner.admit(decision())
                    state, revision = owner.load()
                    state["phase"] = "launching" if launch else "closing"
                    save = owner.save_launch_intent if launch else owner.save
                    before = copy.deepcopy(cloud.items)
                    cloud.before_write = Mock(side_effect=error)
                    with self.assertRaises(type(error)) as caught:
                        save(state, revision)
                    self.assertIs(caught.exception, error)
                    self.assertEqual(cloud.items, before)
                    self.assertEqual(len(cloud.writes), 2)  # one admission and one attempted write
                    cloud.before_write = None
                    self.assertEqual(save(state, revision), revision + 1)

    def test_lost_response_and_unreadable_commit_reconciles_without_duplicate_write(self):
        state, revision = self.store.load()
        state["phase"] = "launching"
        error = TimeoutError("write response lost")
        def lost_response():
            self.cloud.read_error = TimeoutError("readback unavailable")
            raise error
        self.cloud.after_commit = lost_response
        with self.assertRaises(TimeoutError) as caught:
            self.store.save_launch_intent(state, revision)
        self.assertIs(caught.exception, error)
        self.cloud.after_commit, self.cloud.read_error = None, None
        recorded, next_revision = self.store.load()
        self.assertEqual((recorded, next_revision), (state, revision + 1))
        with self.assertRaisesRegex(RevisionConflict, "revision"):
            self.store.save_launch_intent(state, revision)
        self.assertEqual(len(self.cloud.writes), 2)

    def test_no_commit_and_unreadable_readback_retries_only_after_exact_reload(self):
        state, revision = self.store.load()
        state["phase"] = "closing"
        error = ConnectionResetError("write reset")
        def lost_request():
            self.cloud.read_error = TimeoutError("readback unavailable")
            raise error
        self.cloud.before_write = lost_request
        with self.assertRaises(ConnectionResetError) as caught:
            self.store.save(state, revision)
        self.assertIs(caught.exception, error)
        self.cloud.before_write, self.cloud.read_error = None, None
        old_state, old_revision = self.store.load()
        self.assertEqual((old_state["phase"], old_revision), ("armed", revision))
        self.assertEqual(self.store.save(state, old_revision), revision + 1)
        self.assertEqual(len(self.cloud.writes), 3)

    def test_read_conflict_is_deferred_without_write_or_owner_change(self):
        before = copy.deepcopy(self.cloud.items)
        error = cancellation("None", "TransactionConflict")
        self.cloud.read_error = error
        for operation in (self.store.load, lambda: self.store.admit(decision())):
            with self.subTest(operation=operation), self.assertRaises(ClientError) as caught:
                operation()
            self.assertIs(caught.exception, error)
            self.assertTrue(retryable_io(caught.exception))
        self.assertEqual(self.cloud.items, before)
        self.assertEqual(len(self.cloud.writes), 1)

    def test_mixed_write_failure_does_not_become_transient_when_readback_fails(self):
        state, revision = self.store.load()
        state["phase"] = "launching"
        error = cancellation("ThrottlingError", "ConditionalCheckFailed")
        def rejected():
            self.cloud.read_error = TimeoutError("readback unavailable")
            raise error
        self.cloud.before_write = rejected
        with self.assertRaises(ReconciliationRequired) as caught:
            self.store.save_launch_intent(state, revision)
        self.assertFalse(retryable_io(caught.exception))
        self.assertEqual(self.cloud.items[self.store.pk]["revision"], {"N": str(revision)})

    def test_changed_owner_and_verified_revision_conflict_have_distinct_outcomes(self):
        for changed in ("owner", "revision"):
            with self.subTest(changed=changed):
                cloud = DynamoTransactions()
                owner = store(cloud)
                owner.admit(decision())
                state, revision = owner.load()
                state["phase"] = "closing"
                def lost_response():
                    if changed == "owner":
                        cloud.items["active"]["epoch"] = {"S": "f" * 64}
                    else:
                        cloud.items[owner.pk]["revision"] = {"N": str(revision + 2)}
                    raise TimeoutError("write response lost")
                cloud.after_commit = lost_response
                expected = ReconciliationRequired if changed == "owner" else RevisionConflict
                with self.assertRaises(expected) as caught:
                    owner.save(state, revision)
                self.assertEqual(transient(caught.exception), changed == "revision")
                self.assertEqual(len(cloud.writes), 2)

    def test_corrupt_readback_is_not_hidden_by_transport_error(self):
        state, revision = self.store.load()
        state["phase"] = "closing"
        def lost_response():
            self.cloud.items[self.store.pk]["revision"] = {"N": "invalid"}
            raise TimeoutError("write response lost")
        self.cloud.after_commit = lost_response
        with self.assertRaises(ReconciliationRequired) as caught:
            self.store.save(state, revision)
        self.assertFalse(retryable_io(caught.exception))

    def test_retirement_no_commit_throttle_retains_owner_then_reconciles(self):
        state, revision = self.store.load()
        state["phase"] = "complete"
        revision = self.store.save(state, revision)
        error = api_error("ThrottlingException")
        self.cloud.before_write = Mock(side_effect=error)
        with self.assertRaises(ClientError) as caught:
            self.store.retire("d" * 64, revision)
        self.assertIs(caught.exception, error)
        self.assertIn("active", self.cloud.items)
        self.assertFalse(self.store.read().closed)
        self.cloud.before_write = None
        self.assertTrue(self.store.retire("d" * 64, revision).closed)
        writes = len(self.cloud.writes)
        self.assertTrue(self.store.retire("d" * 64, revision).closed)
        self.assertEqual(len(self.cloud.writes), writes)

    def test_retirement_lost_response_and_readback_preserves_tombstone_and_next_owner(self):
        state, revision = self.store.load()
        state["phase"] = "complete"
        revision = self.store.save(state, revision)
        error = TimeoutError("retirement response lost")
        def lost_response():
            self.cloud.read_error = TimeoutError("readback unavailable")
            raise error
        self.cloud.after_commit = lost_response
        with self.assertRaises(TimeoutError) as caught:
            self.store.retire("d" * 64, revision)
        self.assertIs(caught.exception, error)
        self.cloud.after_commit, self.cloud.read_error = None, None
        other = decision(run_key="repo-123-run-457", epoch="f" * 64)
        store(self.cloud, other).admit(other)
        before = copy.deepcopy(self.cloud.items)
        self.assertTrue(self.store.retire("d" * 64, revision).closed)
        self.assertEqual(self.cloud.items, before)
        self.assertEqual(self.cloud.items["active"]["run_key"], {"S": other.run_key})

    def test_admission_lost_response_and_readback_reconciles_one_reservation(self):
        cloud = DynamoTransactions()
        owner = store(cloud)
        error = TimeoutError("admission response lost")
        def lost_response():
            cloud.read_error = TimeoutError("readback unavailable")
            raise error
        cloud.after_commit = lost_response
        with self.assertRaises(TimeoutError) as caught:
            owner.admit(decision())
        self.assertIs(caught.exception, error)
        cloud.after_commit, cloud.read_error = None, None
        self.assertEqual(owner.admit(decision()).revision, 0)
        self.assertEqual(len(cloud.writes), 1)
        self.assertEqual(cloud.items["budget#total"]["reserved"], {"N": "79000000"})


if __name__ == "__main__":
    unittest.main()
