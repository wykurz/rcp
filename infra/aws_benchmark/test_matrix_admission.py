"""Matrix admission: explicit ad-hoc selection and an explicit scheduled opt-in; no service calls."""

from dataclasses import asdict
from datetime import timedelta
import unittest
from unittest.mock import Mock

from . import common, production_workflow
from .production_handler import read_policy
from .production_policy import MAX_MANUAL_SUITES, WorkflowRun, authorize, canonical, SCHEDULE_MAIN_AA
from .test_production_admission import Cloud
from .test_production_policy import NOW, SHA, identity, inputs, policy, run_json

FULL = "matrix-all-local"
SINGLE = ("matrix-commission-local", "matrix-1g-local", "matrix-100m-local", "matrix-1m-local", "matrix-1k-local")
EIGHT_HOURS = dict(monthly_cap_microusd=200_000_000, ancillary_microusd=4_947_200)
SLOT = dict(schedule_slots=((3, 19, 30),), scheduled_baseline=SCHEDULE_MAIN_AA)


def weekly(**updates):
    return policy(**(SLOT | EIGHT_HOURS | dict(scheduled_suite=FULL, scheduled_runtime_seconds=28_800) | updates))


def approve(configured, suite, **identity_updates):
    who = identity(**identity_updates)
    return authorize(configured, who, WorkflowRun.verify(run_json(who), who, NOW), inputs(suite=suite), NOW)


def stored(value, **item):
    api = Mock()
    api.get_item.return_value = dict(Item={"pk": {"S": "policy"}, "version": {"S": value["version"]},
        "enabled": {"BOOL": True}, "payload": {"S": canonical(value).decode()}} | item)
    return read_policy(api, "production-table", "483465")


class MatrixAdmissionTests(unittest.TestCase):
    def test_an_ad_hoc_run_selects_any_explicitly_approved_matrix_suite(self):
        configured = policy(manual_suites=("bulk-1g-metadata", *SINGLE))
        for suite in SINGLE:
            with self.subTest(suite=suite):
                result = approve(configured, suite)
                self.assertEqual((result["runtime_seconds"], result["capacity_seconds"], result["reserve_microusd"]),
                                 (10_800, 43_200, 16_052_800))
        for suite in (FULL, "bulk-100m-metadata"):
            with self.subTest(suite=suite), self.assertRaisesRegex(ValueError, "not explicitly approved"):
                approve(configured, suite)
        # the allowlist stays explicit and bounded
        self.assertEqual(MAX_MANUAL_SUITES, 8)
        with self.assertRaisesRegex(ValueError, "manual file-set allowlist"):
            policy(manual_suites=tuple(common.DURABLE_SUITES[:9]))
        with self.assertRaisesRegex(ValueError, "manual file-set allowlist"):
            policy(manual_suites=("matrix-1g-local", "matrix-1g-local"))

    def test_the_full_matrix_by_hand_needs_the_same_opt_in_as_any_eight_hour_run(self):
        opted = dict(manual_suites=("matrix-commission-local", FULL), manual_runtime_seconds=28_800,
                     extended_manual_not_after=(NOW + timedelta(minutes=45)).isoformat()) | EIGHT_HOURS
        result = approve(policy(**opted), FULL)
        self.assertEqual((result["runtime_seconds"], result["capacity_seconds"], result["reserve_microusd"]),
                         (28_800, 7_200, 50_000_000))
        with self.assertRaisesRegex(ValueError, "extended manual qualification is not explicitly approved"):
            policy(**(opted | dict(manual_runtime_seconds=0)))
        with self.assertRaisesRegex(ValueError, "recorded runtime profile"):
            policy(**(opted | dict(manual_runtime_seconds=21_600)))
        with self.assertRaisesRegex(ValueError, "extended manual admission window is closed"):
            approve(policy(**(opted | dict(extended_manual_not_after=NOW.isoformat()))), FULL)
        with self.assertRaisesRegex(ValueError, "not approved"):
            approve(policy(**(opted | dict(manual_actor_ids=("55",)))), FULL, actor_id="56")
        with self.assertRaisesRegex(ValueError, "immutable owner"):
            approve(policy(**(opted | dict(manual_actor_ids=("55",)))), FULL, actor_id="55")

    def test_a_scheduled_matrix_run_is_an_explicit_server_opt_in(self):
        result = approve(weekly(), FULL, event_name="schedule")
        self.assertEqual((result["runtime_seconds"], result["capacity_seconds"], result["reserve_microusd"],
                          result["schedule_slot"]), (28_800, 7_200, 50_000_000, "2026-10-22T19:30Z"))
        self.assertEqual(result["manifest"]["suite"], FULL)
        # exactly one file set is scheduled; the earlier one is no longer admitted
        for suite in ("bulk-1g-metadata", "matrix-1g-local"):
            with self.subTest(suite=suite), self.assertRaisesRegex(ValueError, "scheduled file set is not approved"):
                approve(weekly(), suite, event_name="schedule")
        # scheduling it does not let anyone start it by hand, and needs no expiring manual window
        with self.assertRaisesRegex(ValueError, "not explicitly approved"):
            approve(weekly(), FULL)
        self.assertEqual(weekly().extended_manual_not_after, "")
        # a scheduled run is still only ever the owner's triggering main revision
        with self.assertRaisesRegex(ValueError, "approved owner"):
            approve(weekly(manual_actor_ids=("55",)), FULL, event_name="schedule", actor_id="55")
        with self.assertRaisesRegex(ValueError, "approved baseline and main"):
            who = identity(event_name="schedule")
            authorize(weekly(), who, WorkflowRun.verify(run_json(who), who, NOW),
                      inputs(suite=FULL, baseline="c" * 40, candidate="c" * 40), NOW)

    def test_a_scheduled_eight_hour_suite_needs_its_own_runtime_approval(self):
        for updates, refusal in (
                (dict(scheduled_runtime_seconds=0), "extended scheduled run is not explicitly approved"),
                (dict(scheduled_runtime_seconds=28_801), "invalid extended scheduled runtime"),
                (dict(scheduled_runtime_seconds=True), "invalid extended scheduled runtime"),
                (dict(scheduled_runtime_seconds=4_199), "does not fit its explicit qualification budget"),
                # the eight-hour profile is one fixed runtime, not a range
                (dict(scheduled_runtime_seconds=21_600), "exactly its recorded runtime profile"),
                (dict(scheduled_suite="matrix-everything"), "invalid scheduled file set"),
                (dict(scheduled_suite=None), "invalid scheduled file set"),
                # a runtime approval with no eight-hour suite to apply to is a mistake, not a no-op
                (dict(scheduled_suite="bulk-1g-metadata"), "extended scheduled runtime has no approved file set"),
                (dict(scheduled_suite="matrix-1g-local"), "extended scheduled runtime has no approved file set"),
                # an approved runtime is checked even while no slot is scheduled yet
                (dict(schedule_slots=(), scheduled_runtime_seconds=21_600), "exactly its recorded runtime profile")):
            with self.subTest(updates=updates), self.assertRaisesRegex(ValueError, refusal):
                weekly(**updates)
        # a manual opt-in never stands in for the scheduled one
        with self.assertRaisesRegex(ValueError, "extended scheduled run is not explicitly approved"):
            weekly(scheduled_runtime_seconds=0, manual_suites=(FULL,), manual_runtime_seconds=28_800,
                   extended_manual_not_after=(NOW + timedelta(minutes=45)).isoformat())
        # naming the suite before any slot or runtime is approved changes nothing yet
        named = weekly(schedule_slots=(), scheduled_runtime_seconds=0)
        self.assertEqual((named.scheduled_suite, named.schedule_slots), (FULL, ()))
        with self.assertRaisesRegex(ValueError, "no unique approved schedule slot|not explicitly approved"):
            approve(named, FULL, event_name="schedule")

    def test_a_scheduled_three_hour_matrix_suite_uses_the_ordinary_runtime(self):
        configured = policy(**(SLOT | dict(scheduled_suite="matrix-1g-local")))
        result = approve(configured, "matrix-1g-local", event_name="schedule")
        self.assertEqual((result["runtime_seconds"], result["reserve_microusd"]), (10_800, 16_052_800))

    def test_a_policy_without_the_new_fields_keeps_scheduling_the_one_gigabyte_run(self):
        default = policy(**SLOT)
        self.assertEqual((default.scheduled_suite, default.scheduled_runtime_seconds), ("bulk-1g-metadata", 0))
        self.assertEqual(approve(default, "bulk-1g-metadata", event_name="schedule")["runtime_seconds"], 10_800)
        with self.assertRaisesRegex(ValueError, "scheduled file set is not approved"):
            approve(default, FULL, event_name="schedule")

    def test_stored_policies_written_before_these_fields_load_with_the_old_meaning(self):
        current = asdict(policy(**SLOT, manual_suites=("bulk-1g-metadata", "bulk-100m-metadata")))
        earlier = {key: value for key, value in current.items()
                   if key not in ("scheduled_suite", "scheduled_runtime_seconds")}
        parsed = stored(earlier)
        self.assertEqual((parsed.scheduled_suite, parsed.scheduled_runtime_seconds, parsed.manual_suites),
                         ("bulk-1g-metadata", 0, ("bulk-1g-metadata", "bulk-100m-metadata")))
        full = asdict(weekly(manual_suites=("bulk-1g-metadata", *SINGLE)))
        parsed = stored(full)
        self.assertEqual((parsed.scheduled_suite, parsed.scheduled_runtime_seconds, len(parsed.manual_suites)),
                         (FULL, 28_800, 6))
        # a payload that names only one of the two fields is not a recorded policy shape
        for name in ("scheduled_suite", "scheduled_runtime_seconds"):
            partial = {key: value for key, value in full.items() if key != name}
            with self.subTest(missing=name), self.assertRaisesRegex(ValueError, "server admission policy unavailable"):
                stored(partial)
        with self.assertRaisesRegex(ValueError, "server admission policy unavailable"):
            stored(full | dict(manual_suites=list(common.DURABLE_SUITES[:9])))

    def test_admission_reserves_a_scheduled_matrix_run_once(self):
        cloud = Cloud()
        cloud.policy = weekly()
        cloud.who = identity(event_name="schedule")
        cloud.ddb.items["policy"]["version"]["S"] = cloud.policy.version
        first = cloud.service.begin("not-a-token", inputs(suite=FULL))
        self.assertEqual(first["reserve_microusd"], 50_000_000)
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "114000000")
        request = cloud.service.store(cloud.who).read().state["request"]
        self.assertEqual((request["suite"], request["runtime_seconds"]), (FULL, 28_800))
        self.assertEqual(request["plan"], common.durable_plan_for_suite(FULL))
        self.assertEqual(request["capacity_deadline"], (NOW + timedelta(hours=2)).isoformat())
        # the same run asking again gets its existing reservation, not a second one
        cloud.service.begin("not-a-token", inputs(suite=FULL))
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "114000000")

    def test_admission_refuses_a_hand_started_full_matrix_under_a_schedule_only_policy(self):
        cloud = Cloud()
        cloud.policy = weekly()
        with self.assertRaises(ValueError):
            cloud.service.begin("not-a-token", inputs(suite=FULL))
        cloud.reader.read.assert_not_called()
        self.assertEqual(cloud.ddb.items["budget#total"]["reserved"]["N"], "64000000")


class MatrixWorkflowTests(unittest.TestCase):
    def environment(self, **updates):
        return dict(GITHUB_REPOSITORY="wykurz/rcp", GITHUB_REPOSITORY_ID="655073755", GITHUB_REF="refs/heads/main",
            GITHUB_WORKFLOW_REF=production_workflow.WORKFLOW, GITHUB_ACTOR_ID="483465", GITHUB_RUN_ATTEMPT="1",
            RCP_PRODUCTION_ENABLED="true", RCP_MODE="benchmark", GITHUB_SHA=SHA, GITHUB_WORKFLOW_SHA=SHA,
            GITHUB_EVENT_NAME="workflow_dispatch", RCP_REQUESTED_REVISION="", RCP_REQUESTED_SUITE="") | updates

    def test_a_manual_request_may_name_any_matrix_suite(self):
        for suite in common.MATRIX_SUITES:
            with self.subTest(suite=suite):
                self.assertEqual(production_workflow.selection(self.environment(RCP_REQUESTED_SUITE=suite)), (SHA, suite))
        with self.assertRaisesRegex(ValueError, "not allowlisted"):
            production_workflow.selection(self.environment(RCP_REQUESTED_SUITE="matrix-all-wan"))

    def test_a_scheduled_request_asks_for_the_one_scheduled_suite(self):
        scheduled = dict(GITHUB_EVENT_NAME="schedule", RCP_SCHEDULE_ENABLED="true",
                         RCP_EVENT_SCHEDULE=production_workflow.REGULAR_CRON)
        # the weekly run is the whole matrix: every file set, locally and through rcpd
        self.assertEqual(production_workflow.SCHEDULED_SUITE, "matrix-all-local+remote")
        self.assertEqual(production_workflow.REGULAR_CRON, "17 8 * * 1")
        self.assertIn(production_workflow.SCHEDULED_SUITE, common.DURABLE_SUITES)
        self.assertEqual(len(common.MATRIX_SUITES[production_workflow.SCHEDULED_SUITE]), 8)
        self.assertEqual(production_workflow.selection(self.environment(**scheduled)),
                         (SHA, production_workflow.SCHEDULED_SUITE))
        for other in ("matrix-all-local", "bulk-1g-metadata"):
            with self.assertRaisesRegex(ValueError, "scheduled requests cannot select another file set"):
                production_workflow.selection(self.environment(**scheduled, RCP_REQUESTED_SUITE=other))
        # the retired Monday and Friday schedule is not a schedule any more
        with self.assertRaisesRegex(ValueError, "unknown or missing scheduled event context"):
            production_workflow.selection(self.environment(**dict(scheduled, RCP_EVENT_SCHEDULE="17 8 * * 1,5")))


if __name__ == "__main__":
    unittest.main()
