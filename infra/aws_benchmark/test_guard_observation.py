"""Offline guard observations and bounded clock-boundary admission regressions."""

from contextlib import redirect_stdout
import copy
from datetime import timedelta
import io
import json
import unittest
from unittest.mock import patch

from . import common, durable, durable_aws
from .guard_observation import GuardObservation, GuardStatus, classify_guard
from .test_durable_coordinator import Cloud, NOW
from .test_production_runtime import ProductionCloud


def response(proof):
    return {"Item": {"payload": {"S": json.dumps(proof)}}}


def proof(at=NOW):
    return dict(namespace="benchmark", healthy=True, observed_at=at.isoformat())


def clock(cloud, *, advance_wall=True, advance_monotonic=True):
    elapsed = [0.0]
    cloud.service.monotonic = lambda: elapsed[0]
    cloud.service.retry_until = 40
    def sleep(seconds):
        cloud.sleeps.append(seconds)
        if advance_wall:
            cloud.now += timedelta(seconds=seconds)
        if advance_monotonic:
            elapsed[0] += seconds
    cloud.service.sleep = sleep
    return elapsed


def guard_reads(cloud):
    return [call.kwargs for call in cloud.api["dynamodb"].get_item.call_args_list
            if call.kwargs["Key"] == {"pk": {"S": "guard"}}]


class GuardClassificationTests(unittest.TestCase):
    def test_exact_age_boundaries_and_signed_fractional_future(self):
        for age, status in ((-3600, GuardStatus.FUTURE), (-0.173948, GuardStatus.FUTURE),
                            (0, GuardStatus.FRESH), (120, GuardStatus.FRESH),
                            (120.000001, GuardStatus.STALE)):
            with self.subTest(age=age):
                value = proof(NOW - timedelta(seconds=age))
                observation = classify_guard(response(value), namespace="benchmark", now=NOW)
                self.assertEqual(observation.status, status)
                self.assertEqual(observation.observed_at, value["observed_at"])
                self.assertEqual(observation.age_seconds, age)

    def test_freshness_uses_the_durable_guard_bound(self):
        with patch.object(durable, "GUARD_GAP_SECONDS", 7):
            self.assertEqual(classify_guard(response(proof(NOW - timedelta(seconds=7))),
                namespace="benchmark", now=NOW).status, GuardStatus.FRESH)
            self.assertEqual(classify_guard(response(proof(NOW - timedelta(seconds=7, microseconds=1))),
                namespace="benchmark", now=NOW).status, GuardStatus.STALE)

    def test_invalid_proofs_are_sanitized_before_age_classification(self):
        secret = "foreign-private-payload"
        invalid = [None, [], {"Item": []}, {"Item": {}}, {"Item": {"payload": {"S": "{" + secret}}},
                   response([]), response({}), response({**proof(), "namespace": secret}),
                   response({**proof(), "healthy": False}), response({**proof(), "healthy": 1}),
                   response({**proof(), "observed_at": secret}), response({**proof(), "observed_at": None}),
                   response({**proof(), "observed_at": NOW.replace(tzinfo=None).isoformat()}),
                   response({**proof(), "observed_at": "2026-10-21\n12:00:00+00:00"}),
                   {"Item": {"payload": {"S": json.dumps(proof())[:-1] + ',"healthy":true}'}}},
                   {"Item": {"payload": {"S": json.dumps(proof())[:-1] + ',"extra":NaN}'}}}]
        for value in invalid:
            with self.subTest(value=value):
                observation = classify_guard(value, namespace="benchmark", now=NOW)
                self.assertEqual(observation, GuardObservation(GuardStatus.INVALID))
                self.assertNotIn(secret, repr(observation))
        for changes in ({"namespace": secret}, {"healthy": False}):
            observation = classify_guard(response({**proof(NOW + timedelta(hours=1)), **changes}),
                                         namespace="benchmark", now=NOW)
            self.assertEqual(observation, GuardObservation(GuardStatus.INVALID))

    def test_absent_guard_has_no_timestamp_or_age(self):
        for value in ({}, {"Item": None}):
            self.assertEqual(classify_guard(value, namespace="benchmark", now=NOW),
                             GuardObservation(GuardStatus.MISSING))


class GuardDiagnosticTests(unittest.TestCase):
    def test_future_diagnostic_preserves_subsecond_failure_and_post_read_clock(self):
        cloud = Cloud()
        clock(cloud, advance_wall=False)
        cloud.guard = proof(NOW + timedelta(microseconds=173948))
        read = cloud.api["dynamodb"].get_item.side_effect
        cloud.api["dynamodb"].get_item.side_effect = lambda **parameters: {
            **read(**parameters), "ResponseMetadata": {"RequestId": "safe-request-123"}}
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(durable_aws.AllocationDeferred):
            cloud.service.fresh_guard()
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(records), durable_aws.GUARD_READ_ATTEMPTS)
        for attempt, record in enumerate(records, 1):
            self.assertEqual(record, dict(event="guard_observation", status="future", attempt=attempt,
                observed_at=cloud.guard["observed_at"], age_seconds=-0.173948,
                checked_at=NOW.isoformat(timespec="microseconds"), request_id="safe-request-123"))
            self.assertLess(len(json.dumps(record)), 400)

    def test_fresh_read_is_silent_and_pending_logs_only_the_nonfresh_read(self):
        cloud = Cloud()
        clock(cloud)
        output = io.StringIO()
        with redirect_stdout(output):
            cloud.service.fresh_guard()
        self.assertEqual(output.getvalue(), "")
        cloud.guard = proof(NOW + timedelta(microseconds=173948))
        with redirect_stdout(output):
            cloud.service.fresh_guard()
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([record["status"] for record in records], ["future"])
        self.assertEqual(len(guard_reads(cloud)), 3)

    def test_missing_stale_and_invalid_diagnostics_never_retain_secretlike_inputs(self):
        secret = "Bearer private-token.do-not-log"
        for value, status in (({}, "missing"),
                              (response(proof(NOW - timedelta(seconds=121))), "stale"),
                              ({"Item": {"payload": {"S": "malformed " + secret}}}, "invalid"),
                              (response({**proof(), "namespace": secret}), "invalid"),
                              (response({**proof(), "healthy": False, "extra": secret}), "invalid"),
                              (response({**proof(), "observed_at": secret}), "invalid")):
            for metadata in ({"RequestId": secret, "extra": secret},
                             {"RequestId": "a" * 65}, {"RequestId": "request\n" + secret},
                             {"RequestId": {"S": secret}}, secret):
                with self.subTest(status=status, metadata=metadata):
                    cloud = Cloud()
                    cloud.api["dynamodb"].get_item.side_effect = None
                    cloud.api["dynamodb"].get_item.return_value = {**value, "ResponseMetadata": metadata}
                    output = io.StringIO()
                    with redirect_stdout(output), self.assertRaises((ValueError, durable_aws.AllocationDeferred)):
                        cloud.service.fresh_guard()
                    self.assertNotIn(secret, output.getvalue())
                    records = [json.loads(line) for line in output.getvalue().splitlines()]
                    self.assertEqual(len(records), 1)
                    record = records[0]
                    self.assertEqual(set(record), {"event", "status", "observed_at", "age_seconds",
                                                   "checked_at", "attempt"})
                    self.assertEqual(record["status"], status)
                    self.assertEqual(record["checked_at"], NOW.isoformat(timespec="microseconds"))
                    if status == "stale":
                        self.assertEqual(record["age_seconds"], 121)
                    else:
                        self.assertIsNone(record["observed_at"])
                        self.assertIsNone(record["age_seconds"])
                    self.assertLess(len(output.getvalue()), 400)

    def test_diagnostic_uses_the_same_post_read_sample_as_classification(self):
        cloud = Cloud()
        clock(cloud, advance_wall=False)
        cloud.guard = proof(NOW + timedelta(microseconds=173948))
        sampled = []
        def now():
            sampled.append(NOW)
            return NOW
        cloud.service.now = now
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(durable_aws.AllocationDeferred):
            cloud.service.fresh_guard()
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(sampled), len(guard_reads(cloud)))
        self.assertTrue(all(record["checked_at"] == NOW.isoformat(timespec="microseconds") for record in records))

    def test_reusable_diagnostic_rejects_unbounded_attempts_without_echoing_them(self):
        observation = classify_guard({}, namespace="benchmark", now=NOW)
        for attempt in (0, durable_aws.GUARD_READ_ATTEMPTS + 1, True, "private-token", 10 ** 100):
            with self.subTest(attempt=attempt), self.assertRaisesRegex(ValueError, "invalid guard diagnostic attempt"):
                observation.diagnostic(checked_at=NOW, attempt=attempt)


class GuardReaderTests(unittest.TestCase):
    def test_small_future_requires_a_consistent_reread_before_freshness(self):
        cloud = Cloud()
        clock(cloud)
        cloud.guard = proof(NOW + timedelta(microseconds=173948))
        original = copy.deepcopy(cloud.guard)
        read = cloud.api["dynamodb"].get_item.side_effect
        def get(**parameters):
            self.assertEqual(cloud.events, [])
            self.assertEqual(cloud.state["attempts"], 0)
            return read(**parameters)
        cloud.api["dynamodb"].get_item.side_effect = get
        observed = cloud.service.fresh_guard()
        self.assertEqual(observed, original["observed_at"])
        self.assertEqual(cloud.guard, original)
        self.assertEqual(cloud.sleeps, [durable_aws.GUARD_RETRY_DELAY])
        self.assertEqual(len(guard_reads(cloud)), 2)
        self.assertTrue(all(call == dict(TableName=cloud.config.table, Key={"pk": {"S": "guard"}},
                                        ConsistentRead=True) for call in guard_reads(cloud)))
        self.assertEqual(cloud.events, [])

    def test_clock_is_sampled_after_each_read(self):
        cloud = Cloud()
        cloud.guard = proof(NOW + timedelta(microseconds=173948))
        read = cloud.api["dynamodb"].get_item.side_effect
        def get(**parameters):
            value = read(**parameters)
            cloud.now += timedelta(seconds=1)
            return value
        cloud.api["dynamodb"].get_item.side_effect = get
        self.assertEqual(cloud.service.fresh_guard(), cloud.guard["observed_at"])
        self.assertEqual(cloud.sleeps, [])
        self.assertEqual(len(guard_reads(cloud)), 1)

    def test_future_retry_reclassifies_missing_stale_foreign_unhealthy_and_malformed(self):
        for changed, error in ((None, durable_aws.AllocationDeferred),
                               (proof(NOW - timedelta(seconds=121)), durable_aws.AllocationDeferred),
                               ({**proof(), "namespace": "foreign-private-payload"}, ValueError),
                               ({**proof(), "healthy": False}, ValueError), ([], ValueError)):
            with self.subTest(changed=changed):
                cloud = Cloud()
                clock(cloud)
                cloud.guard = proof(NOW + timedelta(microseconds=173948))
                sleep = cloud.service.sleep
                def change(seconds):
                    sleep(seconds)
                    cloud.guard = changed
                cloud.service.sleep = change
                with self.assertRaises(error) as failure:
                    cloud.service.fresh_guard()
                self.assertNotIn("foreign-private-payload", str(failure.exception))
                self.assertEqual(len(guard_reads(cloud)), 2)
                self.assertEqual(len(cloud.sleeps), 1)
                self.assertEqual(cloud.events, [])

    def test_stale_missing_and_invalid_initial_scans_are_not_retried(self):
        for value, error in ((None, durable_aws.AllocationDeferred),
                             (proof(NOW - timedelta(seconds=121)), durable_aws.AllocationDeferred),
                             ({**proof(), "healthy": False}, ValueError)):
            with self.subTest(value=value):
                cloud = Cloud()
                clock(cloud)
                cloud.guard = value
                with self.assertRaises(error):
                    cloud.service.fresh_guard()
                self.assertEqual(len(guard_reads(cloud)), 1)
                self.assertEqual(cloud.sleeps, [])

    def test_future_calls_are_finite_with_frozen_or_backwards_clocks(self):
        for wall, monotonic in ((False, False), (True, False), (False, True), (True, True)):
            with self.subTest(wall=wall, monotonic=monotonic):
                cloud = Cloud()
                elapsed = clock(cloud, advance_wall=wall, advance_monotonic=monotonic)
                cloud.guard = proof(NOW + timedelta(hours=1))
                if wall and monotonic:
                    sleep = cloud.service.sleep
                    def backwards(seconds):
                        sleep(seconds)
                        cloud.now -= timedelta(seconds=10)
                        elapsed[0] -= 10
                    cloud.service.sleep = backwards
                with self.assertRaises(durable_aws.AllocationDeferred):
                    cloud.service.fresh_guard()
                self.assertEqual(len(guard_reads(cloud)), durable_aws.GUARD_READ_ATTEMPTS)
                self.assertEqual(len(cloud.sleeps), durable_aws.GUARD_READ_ATTEMPTS - 1)
                self.assertLessEqual(sum(cloud.sleeps), durable_aws.GUARD_RETRY_SECONDS)
                self.assertEqual(cloud.events, [])

    def test_exhausted_budget_or_sdk_read_does_not_start_a_retry(self):
        for exhausted in ("invocation", "read", "sleep"):
            with self.subTest(exhausted=exhausted):
                cloud = Cloud()
                elapsed = clock(cloud)
                cloud.guard = proof(NOW + timedelta(hours=1))
                if exhausted == "invocation":
                    cloud.service.retry_until = 16 + durable_aws.GUARD_RETRY_DELAY
                elif exhausted == "read":
                    read = cloud.api["dynamodb"].get_item.side_effect
                    def slow_read(**parameters):
                        elapsed[0] += durable_aws.GUARD_RETRY_SECONDS + 1
                        return read(**parameters)
                    cloud.api["dynamodb"].get_item.side_effect = slow_read
                else:
                    def slow_sleep(seconds):
                        cloud.sleeps.append(seconds)
                        elapsed[0] += durable_aws.GUARD_RETRY_SECONDS + 1
                    cloud.service.sleep = slow_sleep
                with self.assertRaises(durable_aws.AllocationDeferred):
                    cloud.service.fresh_guard()
                self.assertEqual(len(guard_reads(cloud)), 1)
                self.assertEqual(len(cloud.sleeps), int(exhausted == "sleep"))


class GuardAdmissionTests(unittest.TestCase):
    def test_invalid_scans_do_not_mutate_either_allocation_path(self):
        for cloud_type in (Cloud, ProductionCloud):
            for final_read in (False, True):
                for variant in ("future", "stale", "missing", "malformed", "unhealthy", "foreign", "backwards"):
                    with self.subTest(cloud=cloud_type.__name__, final_read=final_read, variant=variant):
                        cloud = cloud_type()
                        clock(cloud)
                        initial = copy.deepcopy(cloud.state)
                        before = copy.deepcopy(cloud.events)
                        transactions = cloud.api["dynamodb"].transact_write_items.call_count
                        read = cloud.api["dynamodb"].get_item.side_effect
                        count = [0]
                        def get(**parameters):
                            if parameters["Key"] == {"pk": {"S": "guard"}}:
                                count[0] += 1
                                if not final_read or count[0] > 1:
                                    if variant == "future":
                                        cloud.guard = proof(NOW + timedelta(hours=1))
                                    elif variant == "stale":
                                        cloud.guard = proof(NOW - timedelta(seconds=121))
                                    elif variant == "missing":
                                        cloud.guard = None
                                    elif variant == "malformed":
                                        cloud.guard = []
                                    elif variant == "unhealthy":
                                        cloud.guard = {**proof(), "healthy": False}
                                    elif variant == "foreign":
                                        cloud.guard = {**proof(), "namespace": "other"}
                                    else:
                                        cloud.now = NOW - timedelta(seconds=2)
                            return read(**parameters)
                        cloud.api["dynamodb"].get_item.side_effect = get
                        error = ValueError if variant in ("malformed", "unhealthy", "foreign") else durable_aws.AllocationDeferred
                        with self.assertRaises(error):
                            cloud.coordinator()
                        self.assertEqual(cloud.state, initial)
                        self.assertEqual(cloud.events, before)
                        self.assertEqual(cloud.launches, [])
                        self.assertFalse(any(call[0] == "update_item" for call in
                                             cloud.api["dynamodb"].method_calls))
                        cloud.api["dynamodb"].put_item.assert_not_called()
                        self.assertEqual(cloud.api["dynamodb"].transact_write_items.call_count, transactions)

    def test_small_future_allows_launch_only_after_fresh_read_without_changing_identity(self):
        for cloud_type in (Cloud, ProductionCloud):
            with self.subTest(cloud=cloud_type.__name__):
                cloud = cloud_type()
                clock(cloud)
                original = copy.deepcopy(cloud.request)
                event = copy.deepcopy(cloud.event)
                cloud.guard = proof(NOW + timedelta(microseconds=173948))
                sleep = cloud.service.sleep
                def pending(seconds):
                    self.assertEqual(cloud.launches, [])
                    self.assertEqual(cloud.state["attempts"], 0)
                    self.assertFalse(cloud.state["allocation_consumed"])
                    sleep(seconds)
                cloud.service.sleep = pending
                cloud.coordinator()
                self.assertEqual(cloud.request, original)
                self.assertEqual(cloud.state["request"], original)
                self.assertEqual(cloud.event, event)
                self.assertEqual(len(cloud.launches), 1)
                self.assertEqual(cloud.state["attempts"], 1)
                self.assertEqual(cloud.launches[0], cloud.state["attempt"]["parameters"])
                self.assertEqual(cloud.state["attempt"]["close_at"], common.timestamp(
                    common.utc(cloud.state["attempt"]["started_at"]) + timedelta(seconds=cloud.request["runtime_seconds"])))

    def test_future_wait_cannot_extend_capacity_deadline(self):
        cloud = Cloud()
        clock(cloud)
        original = copy.deepcopy(cloud.request)
        cloud.now = common.utc(cloud.request["capacity_deadline"]) - timedelta(seconds=0.1)
        cloud.guard = proof(cloud.now + timedelta(microseconds=173948))
        cloud.coordinator()
        self.assertEqual(cloud.request, original)
        self.assertEqual(cloud.state["request"], original)
        self.assertEqual(cloud.state["phase"], "closing")
        self.assertEqual(cloud.launches, [])
        self.assertEqual(cloud.state["attempts"], 0)

    def test_future_production_replay_holds_the_exact_saved_attempt(self):
        cloud = ProductionCloud()
        cloud.launch_error = TimeoutError()
        cloud.coordinator()
        initial = copy.deepcopy(cloud.state)
        launches = copy.deepcopy(cloud.launches)
        clock(cloud)
        cloud.guard = proof(NOW + timedelta(hours=1))
        with self.assertRaises(durable_aws.AllocationDeferred):
            cloud.coordinator()
        self.assertEqual(cloud.state, initial)
        self.assertEqual(cloud.launches, launches)
        cloud.guard = proof(cloud.now + timedelta(microseconds=173948))
        cloud.coordinator()
        self.assertEqual(cloud.state["attempt"], initial["attempt"])
        self.assertEqual(cloud.state["request"], initial["request"])
        self.assertEqual(cloud.state["attempts"], initial["attempts"])
        self.assertGreater(len(cloud.launches), len(launches))
        self.assertTrue(all(parameters == initial["attempt"]["parameters"] for parameters in cloud.launches))

    def test_future_wait_cannot_extend_a_production_replay_cutoff(self):
        cloud = ProductionCloud()
        cloud.launch_error = TimeoutError()
        cloud.coordinator()
        initial = copy.deepcopy(cloud.state)
        launches = copy.deepcopy(cloud.launches)
        clock(cloud)
        cloud.now = common.utc(initial["attempt"]["close_at"]) - timedelta(seconds=0.1)
        cloud.guard = proof(cloud.now + timedelta(microseconds=173948))
        cloud.coordinator()
        self.assertEqual(cloud.state, initial)
        self.assertEqual(cloud.launches, launches)

    def test_launch_gate_uses_one_clock_sample_and_rejects_a_backward_clock(self):
        cloud = Cloud()
        allocation = durable.attempt(cloud.request, 1, NOW)
        cloud.service.now = lambda: NOW - timedelta(microseconds=1)
        self.assertFalse(cloud.service.launch_allowed(cloud.request, NOW.isoformat(), allocation))
        samples = [NOW, common.utc(cloud.request["capacity_deadline"]) + timedelta(seconds=1)]
        cloud.service.now = lambda: samples.pop(0)
        self.assertTrue(cloud.service.launch_allowed(cloud.request, NOW.isoformat(), allocation))
        self.assertEqual(len(samples), 1)


if __name__ == "__main__":
    unittest.main()
