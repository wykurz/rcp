"""Optimistic conflicts defer; uncertain ownership and failed authorization never do."""

import copy
import json
import os
import unittest
from unittest.mock import Mock, patch

from .common import retryable_io
from .production_auth import GitHubIdentityError, GitHubIdentityTransientError
from .production_errors import RevisionConflict, transient
from .production_github import GitHubRunError, GitHubTransientError, GitHubRateLimitError
from .production_store import ReconciliationRequired
from .test_production_errors import api_error, cancellation
from .test_production_runtime import ProductionCloud
from .test_production_store import DynamoTransactions, decision, store


class ProductionTransientTests(unittest.TestCase):
    def test_only_typed_production_failures_extend_io_classification(self):
        for error in (RevisionConflict("reload"), GitHubTransientError(), GitHubIdentityTransientError(),
                      GitHubRateLimitError(60), TimeoutError("read failed")):
            with self.subTest(error=error):
                self.assertTrue(transient(error))
        for error in (GitHubRunError("invalid metadata"), GitHubIdentityError("invalid token"),
                      ReconciliationRequired("unproven state"), ValueError("state is stale"),
                      api_error("AccessDenied"), cancellation("ConditionalCheckFailed", "ThrottlingError")):
            error.__cause__ = TimeoutError("unrelated prior failure")
            with self.subTest(error=error):
                self.assertFalse(transient(error))
        self.assertFalse(retryable_io(RevisionConflict("not a transport failure")))


class RevisionConflictTests(unittest.TestCase):
    def setUp(self):
        self.cloud = DynamoTransactions()
        self.store = store(self.cloud)
        self.store.admit(decision())

    def advance(self):
        item = self.cloud.items[self.store.pk]
        item["revision"] = {"N": str(int(item["revision"]["N"]) + 1)}

    def test_same_owner_pre_read_conflict_does_not_write_and_reloads_before_retry(self):
        state, revision = self.store.load()
        self.advance()
        with self.assertRaises(RevisionConflict):
            self.store.save(state, revision)
        self.assertEqual(len(self.cloud.writes), 1)
        current, revision = self.store.load()
        current["phase"] = "closing"
        self.assertEqual(self.store.save(current, revision), revision + 1)

    def test_transaction_revision_fence_rejection_is_retryable_after_valid_snapshot(self):
        state, revision = self.store.load()
        state["phase"] = "closing"
        def raced():
            self.advance()
            raise cancellation("None", "ConditionalCheckFailed")
        self.cloud.before_write = raced
        with self.assertRaises(RevisionConflict):
            self.store.save(state, revision)
        self.assertEqual(self.store.read().state["phase"], "armed")
        self.assertEqual(len(self.cloud.writes), 2)

    def test_revision_advance_cannot_hide_permanent_or_other_condition_failure(self):
        for error in (api_error("AccessDeniedException"), api_error("ValidationException"),
                      cancellation("ConditionalCheckFailed", "ConditionalCheckFailed"),
                      cancellation("None", "ConditionalCheckFailed", "ConditionalCheckFailed"),
                      cancellation("None", "ValidationError"), cancellation("None")):
            with self.subTest(error=error):
                state, revision = self.store.load()
                state["phase"] = "closing"
                def rejected():
                    self.advance()
                    raise error
                self.cloud.before_write = rejected
                with self.assertRaises(ReconciliationRequired) as caught:
                    self.store.save(state, revision)
                self.assertFalse(transient(caught.exception))

    def test_closed_terminal_wrong_owner_bad_request_and_future_revision_are_not_conflicts(self):
        for variant in ("closed", "terminal", "owner", "record-epoch", "corrupt", "request", "future", "boolean"):
            with self.subTest(variant=variant):
                cloud = DynamoTransactions()
                owner = store(cloud)
                owner.admit(decision())
                state, revision = owner.load()
                item = cloud.items[owner.pk]
                if variant in ("closed", "terminal"):
                    complete = dict(state, phase="complete")
                    new_revision = owner.save(complete, revision)
                    if variant == "closed":
                        owner.retire("d" * 64, new_revision)
                else:
                    item["revision"] = {"N": "1"}
                    if variant == "owner":
                        cloud.items["active"]["epoch"] = {"S": "f" * 64}
                    elif variant == "record-epoch":
                        item["epoch"] = {"S": "f" * 64}
                    elif variant == "corrupt":
                        item["revision"] = {"N": "invalid"}
                    elif variant == "request":
                        state["request"]["pins"]["image"] = "changed"
                    elif variant == "future":
                        revision = 2
                    elif variant == "boolean":
                        revision = False
                writes = len(cloud.writes)
                with self.assertRaises(ValueError) as caught:
                    owner.save(state, revision)
                self.assertFalse(transient(caught.exception))
                self.assertEqual(len(cloud.writes), writes)

    def test_newly_committed_marker_or_output_requires_stale_writer_to_reload(self):
        for key, value in (("launch_replayed", True), ("worker_output", {"status": {}, "results": None})):
            with self.subTest(key=key):
                cloud = DynamoTransactions()
                owner = store(cloud)
                owner.admit(decision())
                stale, revision = owner.load()
                owner.save(dict(stale, **{key: value}), revision)
                with self.assertRaises(RevisionConflict):
                    owner.save(dict(stale, phase="closing"), revision)
                self.assertEqual(owner.read().state[key], value)
                current, next_revision = owner.load()
                self.assertEqual(owner.save(dict(current, phase="closing"), next_revision), next_revision + 1)

    def test_committed_worker_output_is_preserved_by_every_later_save(self):
        commitment = {"status": {"key": "status.json", "version": "v1", "sha256": "a" * 64, "bytes": 1},
                      "results": {"key": "results.zip", "version": "v2", "sha256": "b" * 64, "bytes": 10}}
        state, revision = self.store.load()
        state["worker_output"] = commitment
        revision = self.store.save(state, revision)
        for mutation in ("removed", "none", "version", "digest", "type"):
            with self.subTest(mutation=mutation):
                changed = copy.deepcopy(state)
                changed["phase"] = "closing"
                if mutation == "removed":
                    del changed["worker_output"]
                elif mutation == "none":
                    changed["worker_output"] = None
                elif mutation == "version":
                    changed["worker_output"]["status"]["version"] = "v3"
                elif mutation == "digest":
                    changed["worker_output"]["results"]["sha256"] = "c" * 64
                else:
                    changed["worker_output"]["status"]["bytes"] = True
                writes = len(self.cloud.writes)
                with self.assertRaisesRegex(ValueError, "worker output commitment cannot change"):
                    self.store.save(changed, revision)
                self.assertEqual(len(self.cloud.writes), writes)
                self.assertEqual(self.store.read().state["worker_output"], commitment)
        closing = dict(state, phase="closing")
        revision = self.store.save(closing, revision)
        complete = dict(closing, phase="complete")
        revision = self.store.save(complete, revision)
        self.assertEqual(self.store.retire("d" * 64, revision).state["worker_output"], commitment)

    def test_unreadable_or_terminal_readback_never_creates_revision_conflict(self):
        for variant in ("unreadable", "terminal"):
            with self.subTest(variant=variant):
                cloud = DynamoTransactions()
                owner = store(cloud)
                owner.admit(decision())
                state, revision = owner.load()
                def rejected():
                    if variant == "unreadable":
                        cloud.read_error = TimeoutError("unavailable readback")
                    else:
                        item = cloud.items[owner.pk]
                        item.update(revision={"N": "1"}, terminal={"BOOL": True},
                                    state={"S": json.dumps(dict(state, phase="complete"))})
                    raise cancellation("None", "ConditionalCheckFailed")
                cloud.before_write = rejected
                with self.assertRaises(ReconciliationRequired) as caught:
                    owner.save(state, revision)
                self.assertFalse(transient(caught.exception))


class RuntimeRevisionRaceTests(unittest.TestCase):
    def setUp(self):
        network = patch("socket.create_connection", side_effect=AssertionError("offline tests only"))
        network.start()
        self.addCleanup(network.stop)

    @staticmethod
    def bump(cloud):
        item = cloud.database.items[cloud.owner.pk]
        item["revision"] = {"N": str(int(item["revision"]["N"]) + 1)}

    def test_intent_transaction_race_defers_without_launch(self):
        cloud = ProductionCloud()
        def compete():
            cloud.database.before_write = None
            self.bump(cloud)
        cloud.database.before_write = compete
        with patch.dict(os.environ, RCP_PRODUCTION_ENABLED="true"):
            self.assertEqual(cloud.handler("coordinator")["state"], "wait")
        self.assertEqual(cloud.launches, [])
        self.assertEqual(cloud.state["phase"], "armed")
        self.assertIsNone(cloud.state["close_reason"])

    def test_watchdog_lost_checkpoint_defers_without_terminating_worker(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        def compete():
            cloud.database.before_write = None
            self.bump(cloud)
        cloud.database.before_write = compete
        self.assertEqual(cloud.service.watchdog()["state"], "deferred")
        self.assertIsNone(cloud.state["close_reason"])
        self.assertFalse(any(event[0] == "terminate" for event in cloud.events))
        cloud.service.watchdog()
        self.assertTrue(cloud.state["worker_published"])
        self.assertEqual(len(cloud.launches), 1)

    def test_coordinator_lost_finish_checkpoint_reloads_same_allocation(self):
        cloud = ProductionCloud()
        def compete():
            target = next((json.loads(action["Update"]["ExpressionAttributeValues"][":state"]["S"])
                           for action in cloud.database.writes[-1]["TransactItems"] if "Update" in action and
                           ":state" in action["Update"]["ExpressionAttributeValues"]), None)
            if target and target["launch_outcome"] == "accepted":
                cloud.database.before_write = None
                self.bump(cloud)
        cloud.database.before_write = compete
        self.assertEqual(cloud.coordinator()["state"], "wait")
        self.assertEqual(cloud.state["launch_outcome"], "accepted")
        self.assertEqual(len(cloud.launches), 1)
        self.assertIsNone(cloud.state["close_reason"])
        self.assertEqual(cloud.sleeps, [1])


class LaunchRetryCheckpointTests(unittest.TestCase):
    def unresolved(self):
        cloud = ProductionCloud()
        state, revision = cloud.owner.load()
        from . import durable
        allocation = durable.attempt(state["request"], 1, cloud.now)
        allocation["parameters"] = cloud.service.launch_parameters(state["request"], allocation)
        state.update(phase="launching", attempt=allocation, attempts=1)
        durable.launch_outcome(state, "uncertain")
        cloud.owner.save_launch_intent(state, revision)
        return cloud

    def test_checkpoint_preserves_allocation_and_rechecks_policy(self):
        cloud = self.unresolved()
        state, revision = cloud.owner.load()
        state["launch_replayed"] = True
        before = copy.deepcopy(state)
        self.assertEqual(cloud.owner.save_launch_retry(state, revision), revision + 1)
        self.assertEqual(cloud.owner.load(), (before, revision + 1))
        for change in ({"enabled": {"BOOL": False}}, {"version": {"S": "rotated"}}):
            with self.subTest(change=change):
                cloud.database.items["policy"].update(change)
                with self.assertRaises(ReconciliationRequired):
                    cloud.owner.save_launch_retry(state, revision + 1)
                self.assertEqual(cloud.owner.read().state, before)
                self.assertEqual(cloud.launches, [])

    def test_lost_retry_response_requires_fresh_policy_acknowledgement(self):
        cloud = self.unresolved()
        state, revision = cloud.owner.load()
        state["launch_replayed"] = True
        cloud.database.after_commit = Mock(side_effect=TimeoutError("response lost"))
        with self.assertRaises(RevisionConflict):
            cloud.owner.save_launch_retry(state, revision)
        cloud.database.after_commit = None
        self.assertEqual(cloud.owner.load(), (state, revision + 1))
        cloud.database.items["policy"]["enabled"] = {"BOOL": False}
        with self.assertRaises(ReconciliationRequired):
            cloud.owner.save_launch_retry(state, revision + 1)
        self.assertEqual(cloud.launches, [])

    def test_other_writer_cannot_certify_policy_rejected_retry(self):
        cloud = self.unresolved()
        state, revision = cloud.owner.load()
        state["launch_replayed"] = True
        def competing_write_then_policy_rejection():
            cloud.database.items[cloud.owner.pk]["state"] = {"S": json.dumps(state)}
            cloud.database.items[cloud.owner.pk]["revision"] = {"N": str(revision + 1)}
            cloud.database.items["policy"]["enabled"] = {"BOOL": False}
            raise cancellation("None", "None", "ConditionalCheckFailed")
        cloud.database.before_write = competing_write_then_policy_rejection
        with self.assertRaises(ReconciliationRequired):
            cloud.owner.save_launch_retry(state, revision)
        self.assertEqual(cloud.owner.load(), (state, revision + 1))
        self.assertEqual(cloud.launches, [])

    def test_sender_marker_is_required_monotonic_and_the_only_permitted_change(self):
        for marker in (None, False, "true", 1):
            with self.subTest(marker=marker):
                cloud = self.unresolved()
                state, revision = cloud.owner.load()
                if marker is not None:
                    state["launch_replayed"] = marker
                with self.assertRaisesRegex(ValueError, "exact unresolved|replay sender marker"):
                    cloud.owner.save_launch_retry(state, revision)
                self.assertNotIn("launch_replayed", cloud.owner.read().state)
        cloud = self.unresolved()
        state, revision = cloud.owner.load()
        state["launch_replayed"] = True
        revision = cloud.owner.save_launch_retry(state, revision)
        self.assertEqual(cloud.owner.save_launch_retry(state, revision), revision + 1)
        self.assertTrue(cloud.owner.read().state["launch_replayed"])
        for marker in (None, False, "true", 1):
            with self.subTest(stored_marker=marker):
                malformed = dict(state, launch_replayed=marker)
                cloud.database.items[cloud.owner.pk]["state"] = {"S": json.dumps(malformed)}
                with self.assertRaisesRegex(ValueError, "replay sender marker"):
                    cloud.owner.save_launch_retry(state, revision + 1)

    def test_generic_save_cannot_remove_or_change_committed_replay_marker(self):
        for marker in (None, False, "true", 1):
            with self.subTest(marker=marker):
                cloud = self.unresolved()
                state, revision = cloud.owner.load()
                state["launch_replayed"] = True
                revision = cloud.owner.save_launch_retry(state, revision)
                next_state = dict(state, phase="uncertain")
                if marker is None:
                    del next_state["launch_replayed"]
                else:
                    next_state["launch_replayed"] = marker
                writes = len(cloud.database.writes)
                with self.assertRaisesRegex(ValueError, "replay sender marker"):
                    cloud.owner.save(next_state, revision)
                self.assertEqual(len(cloud.database.writes), writes)
                self.assertTrue(cloud.owner.read().state["launch_replayed"])
                current = dict(state, phase="closing", close_reason="safe closure")
                self.assertEqual(cloud.owner.save(current, revision), revision + 1)

    def test_malformed_stored_marker_prevents_generic_save(self):
        for marker in (None, False, "true", 1):
            with self.subTest(marker=marker):
                cloud = self.unresolved()
                state, revision = cloud.owner.load()
                corrupt = dict(state, launch_replayed=marker)
                cloud.database.items[cloud.owner.pk]["state"] = {"S": json.dumps(corrupt)}
                writes = len(cloud.database.writes)
                with self.assertRaisesRegex(ValueError, "replay sender marker"):
                    cloud.owner.save(dict(state, phase="uncertain"), revision)
                self.assertEqual(len(cloud.database.writes), writes)

    def test_changed_attempt_observed_instance_or_close_cannot_be_replayed(self):
        for variant in ("parameters", "deadline", "outcome", "consumed", "binding", "ids", "seen", "close"):
            with self.subTest(variant=variant):
                cloud = self.unresolved()
                state, revision = cloud.owner.load()
                state["launch_replayed"] = True
                if variant == "parameters":
                    state["attempt"]["parameters"]["ClientToken"] = "changed"
                elif variant == "deadline":
                    state["attempt"]["close_at"] = "2099-01-01T00:00:00Z"
                else:
                    changes = {"outcome": {"launch_outcome": "accepted"}, "consumed": {"allocation_consumed": False},
                               "binding": {"binding": {}}, "ids": {"known_instance_ids": ["i-" + "1" * 17]},
                               "seen": {"seen": {"instance": {}}}, "close": {"close_reason": "stopped"}}
                    state.update(changes[variant])
                    # also reject unsafe fields already persisted by an independent observer.
                    item = cloud.database.items[cloud.owner.pk]
                    item["state"] = {"S": json.dumps(state)}
                writes = len(cloud.database.writes)
                with self.assertRaisesRegex(ValueError, "exact unresolved|replay sender marker"):
                    cloud.owner.save_launch_retry(state, revision)
                self.assertEqual(len(cloud.database.writes), writes)
                self.assertEqual(cloud.launches, [])


if __name__ == "__main__":
    unittest.main()
