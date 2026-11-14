"""Recorded progress is bounded public metadata, never a retired measurement."""

import base64
import copy
from dataclasses import replace
from datetime import timedelta
import json
import tempfile
import unittest
from unittest.mock import Mock, patch

from . import common, production_progress as progress, production_publication as publication
from .test_production_collection import Fixture
from . import test_production_publication as publication_fixtures


class ProgressTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.fx = Fixture(temporary.name)
        self.state = copy.deepcopy(self.fx.store.record.state)
        self.state["phase"] = "closing"
        self.state["close_reason"] = "allocated instance stopped or terminated"
        self.fx.service.read_worker_status(self.state)
        self.run_key = self.fx.store.run_key

    def commit(self, value):
        raw = publication.collection.encoded(value)
        self.state["worker_output"] = dict(data_base64=base64.b64encode(raw).decode("ascii"),
            status=dict(key=self.fx.paths.status, version="unit-version", bytes=len(raw), sha256=common.digest(raw)))

    def observe(self):
        return progress.observe(self.state, run_key=self.run_key)

    def test_successful_recorded_status_and_deadlines_remain_provisional(self):
        value = self.observe()
        self.assertEqual(value["worker_outcome"], "succeeded")
        self.assertEqual(value["close_category"], "instance_exited")
        self.assertEqual(value["retirement_not_before"], common.timestamp(
            common.utc(self.state["request"]["outer_deadline"]) + timedelta(seconds=900)))
        self.assertEqual(value["capacity_deadline"], self.state["request"]["capacity_deadline"])
        self.assertEqual(value["worker_deadline"], self.state["binding"]["deadline"])
        self.assertNotIn("benchmark_valid", value)
        self.assertNotIn("retired", value)
        self.assertIn("provisional", " ".join(progress.lines(value)))

    def test_failure_without_archive_and_private_error_is_safely_recorded(self):
        self.commit(dict(run=self.state["request"]["run"], state="completed", success=False,
                         error="private-token https://private.invalid/<script>"))
        value = self.observe()
        self.assertEqual(value["worker_outcome"], "failed")
        self.assertNotIn("private", publication.collection.encoded(value).decode())
        self.assertNotIn("private", " ".join(progress.lines(value)))

    def test_missing_status_is_unknown_and_does_not_infer_success_from_closure(self):
        self.state.pop("worker_output")
        self.state["close_reason"] = "worker completed; result validity is separate from physical cleanup"
        self.assertEqual(self.observe()["worker_outcome"], "unknown")

    def test_tampered_status_digest_size_encoding_or_reference_is_unknown(self):
        original = copy.deepcopy(self.state["worker_output"])
        for field, value in (("sha256", "0" * 64), ("bytes", 999999),
                             ("key", self.fx.paths.status.replace("status.json", "other.json")),
                             ("version", "null")):
            self.state["worker_output"] = copy.deepcopy(original)
            self.state["worker_output"]["status"][field] = value
            with self.subTest(field=field):
                self.assertEqual(self.observe()["worker_outcome"], "unknown")
        self.state["worker_output"] = dict(original, data_base64="not-base64")
        self.assertEqual(self.observe()["worker_outcome"], "unknown")

    def test_foreign_worker_or_foreign_request_cannot_report_success(self):
        self.commit(dict(run="999-1", state="completed", success=True, sha256="a" * 64))
        self.assertEqual(self.observe()["worker_outcome"], "unknown")
        self.state["request"]["run"] = "999-1"
        self.assertEqual(self.observe()["worker_outcome"], "unknown")

    def test_nonterminal_nonboolean_or_missing_archive_digest_status_is_unknown(self):
        for changes in (dict(state="running"), dict(success="true"), dict(sha256="bad")):
            value = dict(run=self.state["request"]["run"], state="completed", success=True, sha256="a" * 64)
            value.update(changes)
            self.commit(value)
            with self.subTest(changes=changes):
                self.assertEqual(self.observe()["worker_outcome"], "unknown")

    def test_unknown_private_close_reason_and_guard_exception_are_fixed_categories(self):
        for reason, category in (("<script>secret-value</script>", "other"),
                                 ("guard failure: private-secret", "guard_problem")):
            self.state["close_reason"] = reason
            value = self.observe()
            self.assertEqual(value["close_category"], category)
            self.assertNotIn("secret", json.dumps(value))
            self.assertNotIn("secret", " ".join(progress.lines(value)))

    def test_controller_cleanup_record_is_not_a_current_physical_or_retirement_claim(self):
        value = self.observe()
        self.assertEqual(value["cleanup_stage"], "recorded")
        self.assertEqual(value["instance_termination"], "recorded")
        text = " ".join(progress.lines(value))
        self.assertIn("not a fresh EC2 check", text)
        self.assertIn("ownership retirement remain pending", text)
        self.assertFalse(self.state["cleanup"]["lease_released"])

    def test_missing_partial_or_unresolved_cleanup_never_becomes_verified(self):
        self.state.pop("cleanup")
        self.assertEqual(self.observe()["cleanup_stage"], "not_recorded")
        for cleanup in (dict(verified_at="private"), dict(verified_at="2026-11-13T23:44:37+00:00",
                live_instances=["i-unknown"], unresolved_allocation=False),
                dict(verified_at="2026-11-13T23:44:37+00:00", live_instances=[], unresolved_allocation=True)):
            self.state["cleanup"] = cleanup
            value = self.observe()
            self.assertEqual(value["cleanup_stage"], "in_progress")
            self.assertIsNone(value["cleanup_verified_at"])

    def test_live_or_missing_seen_state_does_not_claim_termination(self):
        for seen in ({}, {"instance": {"state": "running"}}, {"instance": None}):
            self.state["seen"] = seen
            self.assertEqual(self.observe()["instance_termination"], "not_recorded")

    def test_invalid_timestamps_are_unavailable_without_echoing_private_text(self):
        self.state["request"]["capacity_deadline"] = "<secret>"
        self.state["binding"]["deadline"] = "not-a-date"
        self.state["request"]["outer_deadline"] = "<secret>"
        value = self.observe()
        self.assertIsNone(value["capacity_deadline"])
        self.assertIsNone(value["worker_deadline"])
        self.assertIsNone(value["retirement_not_before"])
        self.assertNotIn("secret", json.dumps(value))

    def test_malformed_optional_context_stays_unavailable_instead_of_holding_reporting(self):
        original = copy.deepcopy(self.state)
        for key in ("request", "binding"):
            for value in (None, [], [1], 1, "private-context"):
                self.state = copy.deepcopy(original)
                self.state[key] = value
                with self.subTest(key=key, value=value):
                    projected = self.observe()
                    self.assertEqual(projected["worker_outcome"], "unknown")
                    self.assertNotIn("private-context", json.dumps(projected))
        for value in (None, 1, [], {}):
            self.state = copy.deepcopy(original)
            self.state["request"]["outer_deadline"] = value
            with self.subTest(deadline=value):
                self.assertIsNone(self.observe()["retirement_not_before"])

    def test_artifact_schema_rejects_extra_fields_categories_times_and_retired_claims(self):
        original = self.observe()
        for key, value in (("private", "token"), ("benchmark_valid", True),
                           ("worker_outcome", "private"), ("worker_outcome", []),
                           ("schema", "foreign"), ("worker_deadline", "<script>"),
                           ("cleanup_verified_at", None)):
            changed = dict(original, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                progress.validate(changed)

    def test_projection_performs_no_aws_reads_or_mutations(self):
        with patch.object(self.fx.service, "read_worker_status", side_effect=AssertionError("no reads")),                 patch("socket.create_connection", side_effect=AssertionError("no network")):
            value = self.observe()
        self.assertEqual(value["worker_outcome"], "succeeded")


class ProgressReaderTests(unittest.TestCase):
    def setUp(self):
        publication_fixtures.PublicationTests.setUp(self)
        state = copy.deepcopy(self.fx.store.record.state)
        state.update(phase="closing", close_reason="allocated instance stopped or terminated")
        self.fx.service.read_worker_status(state)
        self.record = replace(self.fx.store.record, state=state, closed=False, terminal=False,
                              retirement_sha256=None)
        self.item = publication_fixtures.stored(self.record)
        self.owner = dict(pk={"S": "active"}, run_key={"S": self.record.run_key}, epoch={"S": self.record.epoch})
        self.db.get_item.side_effect = lambda **kw: {"Item": copy.deepcopy(
            self.owner if kw["Key"]["pk"]["S"] == "active" else self.item)}

    def test_active_reader_projects_real_committed_status_using_only_existing_get_item(self):
        with patch.object(self.reader.s3, "get_object", side_effect=AssertionError("no extra S3 read")):
            value = self.reader.active_observation()
        self.assertEqual(value["progress"]["worker_outcome"], "succeeded")
        self.assertEqual((value["state"], value["retired"], value["phase"]), ("pending", False, "closing"))
        self.assertEqual(len(self.db.get_item.call_args_list), 3)
        self.assertEqual({c.kwargs["Key"]["pk"]["S"] for c in self.db.get_item.call_args_list},
                         {"active", "run#" + self.record.run_key})

    def test_owner_change_during_observation_is_held(self):
        self.db.get_item.side_effect = [{"Item": self.owner}, {"Item": self.item}, {}]
        with self.assertRaises(ValueError):
            self.reader.active_observation()

    def test_malformed_optional_state_does_not_hold_actual_active_reader(self):
        for context in (dict(binding=[1]), dict(binding=None)):
            item = copy.deepcopy(self.item)
            state = json.loads(item["state"]["S"])
            state.update(context)
            item["state"]["S"] = publication.collection.encoded(state).decode()
            self.db.get_item.side_effect = lambda **kw: {"Item": copy.deepcopy(
                self.owner if kw["Key"]["pk"]["S"] == "active" else item)}
            with self.subTest(context=context):
                value = self.reader.active_observation()
                self.assertEqual(value["state"], "pending")
                self.assertEqual(value["progress"]["worker_outcome"], "unknown")

    def test_changed_immutable_request_is_still_held_before_optional_progress(self):
        item = copy.deepcopy(self.item)
        state = json.loads(item["state"]["S"])
        state["request"]["outer_deadline"] = None
        item["state"]["S"] = publication.collection.encoded(state).decode()
        self.db.get_item.side_effect = lambda **kw: {"Item": copy.deepcopy(
            self.owner if kw["Key"]["pk"]["S"] == "active" else item)}
        with self.assertRaisesRegex(ValueError, "immutable request"):
            self.reader.active_observation()

    def test_idle_reader_does_not_invent_a_worker_result(self):
        self.db.get_item.side_effect = lambda **kw: {}
        value = self.reader.active_observation()
        self.assertEqual(value["state"], "idle")
        self.assertNotIn("progress", value)
