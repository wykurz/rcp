"""The twelve-hour allocation of every file set with the other tools; fake AWS and systemd only."""

from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import Mock, patch

from benchmarks import telemetry
from . import common, durable, production_diagnostics as diagnostics, supervisor_notify, worker
from .production_policy import WorkflowRun, authorize
from .test_durable_contracts import NOW, request as legacy_request
from .test_durable_coordinator import Cloud
from .test_matrix_diagnostics import WORKER
from .test_matrix_pages import matrix_example
from .test_matrix_admission import weekly
from .test_production_policy import NOW as ADMITTED, identity, policy, run_json
from .test_production_stack import INSTANCE, TEMPLATE, launch_context, permits
from .test_supervisor_notify import fixture as notifier
from .test_tool_cells import sampled_matrix, tool_inputs

SELECTIONS = ("local+tools", "local+tools+remote", "local+tools+remote+tools")
WITH_TOOLS = tuple("matrix-all-" + scenarios for scenarios in SELECTIONS)
WITHOUT = ("matrix-all-local", "matrix-all-remote", "matrix-all-local+remote")
SUITE = "matrix-all-local+tools+remote+tools"
EIGHT, TWELVE = "all-filesets-8h-v1", "all-filesets-12h-v1"
# the price and the ancillary allowance of the live policy: twelve hours compute to $61.472
MONEY = dict(monthly_cap_microusd=200_000_000, ancillary_microusd=4_947_200)


def contract(suite):
    return common.durable_plan_contract(common.durable_plan_for_suite(suite))


def tools_request(created=NOW, *, suite=SUITE, runtime=43_200):
    old = legacy_request()
    who = {key: old[key] for key in ("run", "baseline", "candidate", "controller")}
    hashes = dict.fromkeys(contract(suite)["binaries"], common.digest(b"identical binary"))
    return durable.request(dict(who, suite=suite), created, hashes, old["pins"],
                           runtime_seconds=runtime, capacity_seconds=7200)


def approve(configured, suite, **who):
    who = identity(**who)
    return authorize(configured, who, WorkflowRun.verify(run_json(who), who, ADMITTED), tool_inputs(suite), ADMITTED)


def by_hand(**updates):
    return policy(**(MONEY | dict(manual_suites=("matrix-commission-local+tools+remote+tools", SUITE),
                                  manual_runtime_seconds=43_200,
                                  extended_manual_not_after=(ADMITTED + timedelta(minutes=45)).isoformat()) | updates))


class PlanTests(unittest.TestCase):
    def test_every_file_set_with_the_tools_gets_twelve_hours_and_a_reservation_to_match(self):
        for suite in WITH_TOOLS:
            with self.subTest(suite=suite):
                plan = contract(suite)
                self.assertEqual((plan["max_runtime"], plan["runtime_profile"], plan["reservation_microusd"]),
                                 (43_200, TWELVE, 65_000_000))
                # the wait for a machine and the time below which the worker declines are unchanged
                self.assertEqual((plan["max_provisioning"], plan["max_attempts"]), (7_200, 120))
                self.assertEqual(common.durable_worker_reserve(common.durable_plan_for_suite(suite)), 3_600)

    def test_the_eight_hour_plans_that_these_suites_were_first_given_stay_recorded(self):
        for suite in WITH_TOOLS:
            with self.subTest(suite=suite):
                current = common.durable_plan_for_suite(suite)
                self.assertEqual(current["version"], suite + "-plan-v2")
                first = dict(current, version=suite + "-plan-v1")
                recorded = common.durable_plan_contract(first, exact=False)
                self.assertEqual((recorded["suite"], recorded["max_runtime"], recorded["runtime_profile"],
                                  recorded["reservation_microusd"]), (suite, 28_800, EIGHT, 50_000_000))
                self.assertEqual(recorded["binaries"], contract(suite)["binaries"])
                # it is read, and no longer admitted
                with self.assertRaisesRegex(ValueError, "not current"):
                    common.durable_plan_contract(first)
        # a request that the first version admitted is still loaded for reconciliation and collection
        current = tools_request()
        admitted = dict(current, plan=dict(current["plan"], version=SUITE + "-plan-v1"), runtime_seconds=28_800,
                        outer_deadline=common.timestamp(common.utc(current["capacity_deadline"]) + timedelta(hours=8)))
        self.assertEqual(durable.validate(admitted, exact=False)["runtime_seconds"], 28_800)
        with self.assertRaisesRegex(ValueError, "not current"):
            durable.validate(admitted)
        # every other matrix suite still has the one version it was given
        for suite in set(common.MATRIX_SUITES) - set(WITH_TOOLS):
            self.assertEqual(common.durable_plan_for_suite(suite)["version"], suite + "-plan-v1")

    def test_the_suites_without_the_tools_and_those_of_one_file_set_keep_their_time(self):
        for suite in (*WITHOUT, common.FULL_FILE_SET_SUITE):
            with self.subTest(suite=suite):
                plan = contract(suite)
                self.assertEqual((plan["max_runtime"], plan["runtime_profile"], plan["reservation_microusd"]),
                                 (28_800, EIGHT, 50_000_000))
        for selection in ("1g", "100m", "1m", "1k", "commission"):
            for scenarios in SELECTIONS:
                with self.subTest(selection=selection, scenarios=scenarios):
                    plan = contract(f"matrix-{selection}-{scenarios}")
                    self.assertEqual(plan["max_runtime"], 10_800)
                    self.assertNotIn("runtime_profile", plan)
                    self.assertNotIn("reservation_microusd", plan)

    def test_a_runtime_profile_means_one_time_in_every_place_that_holds_it(self):
        self.assertEqual({name: dict(profile) for name, profile in durable.RUNTIME_PROFILES.items()},
                         {EIGHT: dict(runtime_seconds=28_800, max_attempts=120),
                          TWELVE: dict(runtime_seconds=43_200, max_attempts=120)})
        used = set()
        for suite in common.DURABLE_SUITES:
            plan = contract(suite)
            profile = plan.get("runtime_profile")
            with self.subTest(suite=suite):
                if profile is None:
                    self.assertEqual(plan["max_runtime"], durable.RUNTIME_SECONDS)
                    continue
                used.add(profile)
                # the plan, the tag on its instance and the expiry scan agree on the time
                self.assertEqual((plan["max_runtime"], plan["max_attempts"]),
                                 tuple(durable.RUNTIME_PROFILES[profile][key] for key in ("runtime_seconds", "max_attempts")))
        self.assertEqual(used, set(durable.RUNTIME_PROFILES))
        # the launch role may tag an instance with each profile, in both statements that tag one
        statements = TEMPLATE["Resources"]["CoordinatorRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        key = "aws:RequestTag/rcp:runtime-profile"
        tagged = [statement["Condition"]["StringEqualsIfExists"][key] for statement in statements
                  if key in statement.get("Condition", {}).get("StringEqualsIfExists", {})]
        self.assertEqual(tagged, [list(durable.RUNTIME_PROFILES)] * 2)
        # and the supervisor grants no extension beyond the longest of them
        self.assertEqual(supervisor_notify.MAX_EXTENSION_SECONDS,
                         max(profile["runtime_seconds"] for profile in durable.RUNTIME_PROFILES.values()))

    def test_what_waits_for_a_run_outlasts_the_longest_one(self):
        # the coordinator's execution observes a run until its clean-up, which follows the search
        # for a machine, the runtime and the drain: no plan may outlast it
        machine = TEMPLATE["Resources"]["CoordinatorStateMachine"]["Properties"]["Definition"]
        horizons = {suite: sum(contract(suite)[key] for key in ("max_provisioning", "max_runtime", "delivery_drain_seconds"))
                    for suite in common.DURABLE_SUITES}
        self.assertEqual(horizons[SUITE], 51_300)
        self.assertLess(max(horizons.values()), machine["TimeoutSeconds"] - 1_800)
        # the Tuesday refresh of the pages follows a twelve-hour run scheduled for the Monday slot,
        # however late it starts: the schedule can change to such a suite as it is
        from .production_policy import MAX_RUN_AGE_SECONDS
        from .production_workflow import REGULAR_CRON
        minute, hour, _, _, weekday = REGULAR_CRON.split()
        slot = datetime(2026, 11, 23, int(hour), int(minute), tzinfo=timezone.utc)
        self.assertEqual((weekday, slot.isoweekday()), ("1", 1))
        latest = slot + timedelta(seconds=3_600 + MAX_RUN_AGE_SECONDS + horizons[SUITE])
        self.assertGreaterEqual((slot + timedelta(days=1)).replace(hour=3, minute=43) - latest, timedelta(hours=2))

    def test_a_request_for_such_a_suite_runs_for_exactly_twelve_hours(self):
        value = tools_request()
        self.assertEqual((value["runtime_seconds"], value["outer_deadline"]),
                         (43_200, common.timestamp(NOW + timedelta(hours=14))))
        for runtime in (28_800, 43_199, 43_201):
            with self.subTest(runtime=runtime), self.assertRaisesRegex(ValueError, "runtime"):
                tools_request(runtime=runtime)
        # and a suite without the tools cannot ask for the longer time
        with self.assertRaisesRegex(ValueError, "runtime does not fit"):
            tools_request(suite="matrix-all-local+remote")
        self.assertEqual(tools_request(suite="matrix-all-local+remote", runtime=28_800)["runtime_seconds"], 28_800)


class InstanceTests(unittest.TestCase):
    def launched(self, **tags):
        cloud = Cloud()
        cloud.request = tools_request()
        instance = cloud.instance(allocation=durable.attempt(cloud.request, 1, NOW))
        for tag in instance["Tags"]:
            tag["Value"] = tags.get(tag["Key"], tag["Value"])
        cloud.instances = [instance]
        cloud.api["dynamodb"].get_item.side_effect = AssertionError("DDB read forbidden")
        return cloud, instance

    def test_the_instance_is_tagged_with_its_profile_and_expires_after_twelve_hours(self):
        cloud, instance = self.launched()
        self.assertEqual(durable.instance_tags(instance)[durable.TAG_RUNTIME_PROFILE], TWELVE)
        self.assertEqual(durable.tagged_expiry(instance, cloud.config.namespace, NOW), NOW + timedelta(hours=12))
        # the independent scan needs the tag alone: eight hours in, the run goes on
        cloud.now = NOW + timedelta(hours=11, minutes=59)
        cloud.service.independent_scan()
        cloud.api["ec2"].terminate_instances.assert_not_called()
        cloud.now = NOW + timedelta(hours=12)
        cloud.service.independent_scan()
        cloud.api["ec2"].terminate_instances.assert_called_once_with(InstanceIds=[instance["InstanceId"]])
        cloud.api["dynamodb"].get_item.assert_not_called()

    def test_a_deadline_beyond_the_profile_of_the_tag_ends_the_instance_at_once(self):
        for profile, seconds in ((TWELVE, 43_201), (EIGHT, 43_200), (EIGHT, 28_801)):
            cloud, _ = self.launched(**{durable.TAG_RUNTIME_PROFILE: profile,
                                        durable.TAG_DEADLINE: common.timestamp(NOW + timedelta(seconds=seconds))})
            with self.subTest(profile=profile, seconds=seconds):
                cloud.service.independent_scan()
                cloud.api["ec2"].terminate_instances.assert_called_once()

    def test_the_launch_role_may_tag_an_instance_with_either_profile_and_no_other(self):
        base = launch_context()
        for profile, allowed in ((EIGHT, True), (TWELVE, True), ("all-filesets-24h-v1", False), ("", False)):
            context = base | {"aws:RequestTag/rcp:runtime-profile": profile, "ec2:CreateAction": "RunInstances",
                              "aws:TagKeys": base["aws:TagKeys"] + [durable.TAG_RUNTIME_PROFILE]}
            for action in ("ec2:RunInstances", "ec2:CreateTags"):
                with self.subTest(profile=profile, action=action):
                    self.assertEqual(permits("CoordinatorRole", action, INSTANCE, context), allowed)


class AdmissionTests(unittest.TestCase):
    def test_by_hand_it_needs_the_twelve_hour_approval_and_reserves_its_fixed_amount(self):
        result = approve(by_hand(), SUITE)
        self.assertEqual((result["runtime_seconds"], result["capacity_seconds"], result["reserve_microusd"]),
                         (43_200, 7_200, 65_000_000))
        with self.assertRaisesRegex(ValueError, "extended manual qualification is not explicitly approved"):
            by_hand(manual_runtime_seconds=0)
        # eight hours are another profile's time, not a shorter run of this suite
        with self.assertRaisesRegex(ValueError, "exactly its recorded runtime profile"):
            by_hand(manual_runtime_seconds=28_800)
        with self.assertRaisesRegex(ValueError, "invalid extended manual runtime"):
            by_hand(manual_runtime_seconds=43_201)
        with self.assertRaisesRegex(ValueError, "extended manual admission window is closed"):
            approve(by_hand(extended_manual_not_after=ADMITTED.isoformat()), SUITE)
        # the suites of one file set in the same list still run for the ordinary three hours
        small = approve(by_hand(), "matrix-commission-local+tools+remote+tools")
        self.assertEqual((small["runtime_seconds"], small["reserve_microusd"]), (10_800, 20_000_000))

    def test_a_price_at_which_twelve_hours_exceed_the_fixed_amount_is_refused(self):
        # $4.608 an hour for twelve hours and sixteen minutes, and the allowance: $61.472
        self.assertEqual(approve(by_hand(), SUITE)["reserve_microusd"], 65_000_000)
        with self.assertRaisesRegex(ValueError, "price exceeds the fixed qualification reservation"):
            approve(by_hand(hourly_microusd=4_896_000), SUITE)
        self.assertEqual(approve(by_hand(hourly_microusd=4_895_000), SUITE)["reserve_microusd"], 65_000_000)

    def test_one_policy_approves_one_extended_runtime_by_hand(self):
        # an eight-hour suite cannot share the by-hand list with a twelve-hour one
        with self.assertRaisesRegex(ValueError, "does not fit its explicit qualification budget"):
            by_hand(manual_suites=(SUITE, "matrix-all-local+remote"))
        with self.assertRaisesRegex(ValueError, "exactly its recorded runtime profile"):
            by_hand(manual_suites=(SUITE, "matrix-all-local+remote"), manual_runtime_seconds=28_800)
        # the three suites with the tools share their time, so they can be listed together
        self.assertEqual({approve(by_hand(manual_suites=WITH_TOOLS), suite)["runtime_seconds"] for suite in WITH_TOOLS},
                         {43_200})

    def test_the_scheduled_eight_hour_run_and_a_twelve_hour_run_by_hand_fit_one_policy(self):
        # how the longer suite is tried once before the schedule changes to it
        both = dict(manual_suites=(SUITE,), manual_runtime_seconds=43_200,
                    extended_manual_not_after=(ADMITTED + timedelta(minutes=45)).isoformat())
        scheduled = approve(weekly(**both), "matrix-all-local", event_name="schedule")
        self.assertEqual((scheduled["runtime_seconds"], scheduled["reserve_microusd"]), (28_800, 50_000_000))
        tried = approve(weekly(**both), SUITE)
        self.assertEqual((tried["runtime_seconds"], tried["reserve_microusd"]), (43_200, 65_000_000))

    def test_scheduling_it_needs_a_twelve_hour_approval_of_its_own(self):
        result = approve(weekly(scheduled_suite=SUITE, scheduled_runtime_seconds=43_200), SUITE, event_name="schedule")
        self.assertEqual((result["runtime_seconds"], result["capacity_seconds"], result["reserve_microusd"]),
                         (43_200, 7_200, 65_000_000))
        for seconds, refusal in ((0, "extended scheduled run is not explicitly approved"),
                                 (28_800, "exactly its recorded runtime profile"),
                                 (43_201, "invalid extended scheduled runtime")):
            with self.subTest(seconds=seconds), self.assertRaisesRegex(ValueError, refusal):
                weekly(scheduled_suite=SUITE, scheduled_runtime_seconds=seconds)
        # the approval by hand never stands in for it
        with self.assertRaisesRegex(ValueError, "extended scheduled run is not explicitly approved"):
            weekly(scheduled_suite=SUITE, scheduled_runtime_seconds=0, manual_suites=(SUITE,),
                   manual_runtime_seconds=43_200, extended_manual_not_after=(ADMITTED + timedelta(minutes=45)).isoformat())


class SupervisorTests(unittest.TestCase):
    def assigned(self, *, runtime=43_200):
        """A window bound to an instance two minutes ago, and the request that goes with it."""
        started = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=120)
        value = tools_request(started)
        window = durable.window(value, dict(instance_id="i-" + "1" * 17,
            launch_at=common.timestamp(started + timedelta(seconds=90)), attempt_id=value["run"] + "-1",
            attempt_started_at=common.timestamp(started),
            deadline=common.timestamp(started + timedelta(seconds=runtime - 300))))
        # consistent in itself, whatever the runtime: only the plan's own time may be extended to
        window.update(runtime_seconds=runtime, outer_deadline=common.timestamp(
            common.utc(value["capacity_deadline"]) + timedelta(seconds=runtime)))
        request = {key: window[key] for key in ("run", "baseline", "candidate", "suite", "deadline")}
        return window, dict(request, sha256=value["sha256"])

    def extend(self, window, request, *, identity=None, sender=True):
        sender, budget = (Mock() if sender else None), object()
        if sender is not None:
            sender.extend.return_value = 42_000_000_000
        with patch.object(worker.os, "geteuid", return_value=0):
            result = worker.extend_supervisor_runtime(window, request, identity or window["instance_id"], budget, sender)
        return result, sender, budget

    def test_the_worker_asks_its_supervisor_for_the_twelve_hours_of_its_plan(self):
        window, request = self.assigned()
        result, sender, budget = self.extend(window, request)
        sender.extend.assert_called_once_with(budget, maximum_seconds=43_200, shutdown_seconds=300)
        self.assertEqual((result["extension_usec"], result["barrier_processed"]), (42_000_000_000, True))

    def test_a_window_with_another_time_than_its_plans_gets_no_extension(self):
        # eight hours were the one extended runtime before: a twelve-hour plan must not take them
        window, request = self.assigned(runtime=28_800)
        with self.assertRaisesRegex(ValueError, "exact instance/runtime/notification binding"):
            self.extend(window, request)
        window, request = self.assigned()
        for refused in (dict(identity="i-" + "2" * 17), dict(sender=False)):
            with self.subTest(refused=refused), self.assertRaisesRegex(ValueError, "exact instance/runtime/notification"):
                self.extend(window, request, **refused)

    def test_the_supervisor_extends_to_twelve_hours_and_no_further(self):
        with notifier(left=50_000) as (sender, budget, library, _, _):
            sender.ready(budget)
            self.assertEqual(sender.extend(budget, maximum_seconds=43_200, shutdown_seconds=300), 43_198_000_000)
            self.assertEqual(library.sd_notify.call_args.args, (0, b"EXTEND_TIMEOUT_USEC=43198000000"))
        with notifier(left=50_000) as (sender, budget, library, _, _):
            sender.ready(budget)
            with self.assertRaisesRegex(ValueError, "invalid supervisor extension bounds"):
                sender.extend(budget, maximum_seconds=43_201, shutdown_seconds=300)
            self.assertEqual(library.sd_notify.call_count, 1)


class SamplerTests(unittest.TestCase):
    """A run is sampled for as long as it may take, and its public companion says how long that is."""

    def test_the_journal_of_a_matrix_run_covers_the_longest_allocation(self):
        longest = max(profile["runtime_seconds"] for profile in durable.RUNTIME_PROFILES.values())
        limits = telemetry.MATRIX_LIMITS
        self.assertEqual(limits["max_seconds"], longest)
        # a sample each period and one at each phase mark
        self.assertGreaterEqual(limits["max_samples"], longest // telemetry.PERIOD_SECONDS + telemetry.MAX_PHASES)
        # a line took 2.7 kB on the AWS worker: 2,144 lines in 5,803,658 bytes, in the first full run
        self.assertGreaterEqual(limits["max_bytes"], limits["max_samples"] * 2_800)
        # a companion names the bounds its run was sampled under: the earlier ones or these
        self.assertEqual(diagnostics.TELEMETRY_DESCRIPTORS, (telemetry.descriptor(), telemetry.descriptor(limits)))
        self.assertEqual(diagnostics.MAX_ALLOCATION_SECONDS, longest)

    def test_a_twelve_hour_run_publishes_its_allocation_and_the_bounds_of_its_journal(self):
        sampled = telemetry.descriptor(telemetry.MATRIX_LIMITS)
        raw, _ = sampled_matrix(SUITE, sampled)
        record, _ = matrix_example(SUITE)
        extra = diagnostics.derive(record, b"archive", raw, dict(WORKER, window=dict(runtime_seconds=43_200)))
        self.assertEqual((extra["status"], extra["reason"]), ("complete", None))
        self.assertEqual(extra["limits"], dict(allocation_seconds=43_200, command_timeout_seconds=3_600))
        self.assertEqual(extra["setup"]["instrumentation"], sampled)
        # with the bounds known, what was sampled is published: the run's phases and each trial's
        self.assertEqual(len(extra["system"]["phases"]), 13)
        self.assertTrue(all(trial["system"]["status"] == "complete" for trial in extra["trials"]))
        diagnostics.validate(extra, record)
        # an allocation beyond the longest there is does not validate
        with self.assertRaisesRegex(ValueError, "invalid diagnostic execution limits"):
            diagnostics.validate(dict(extra, limits=dict(extra["limits"], allocation_seconds=43_201)), record)
        # what is published is the server's own copy of the bounds, never the object in the record:
        # one that only compares equal, with a float where the contract has an integer, changes nothing
        raw["context"]["system_telemetry"] = dict(sampled, period_seconds=10.0)
        again = diagnostics.derive(record, b"archive", raw, dict(WORKER, window=dict(runtime_seconds=43_200)))
        self.assertEqual(json.dumps(again["setup"], sort_keys=True), json.dumps(extra["setup"], sort_keys=True))

    def test_a_summary_may_hold_what_the_widest_journal_can_and_no_more(self):
        raw, _ = sampled_matrix(SUITE, telemetry.descriptor(telemetry.MATRIX_LIMITS))
        record, _ = matrix_example(SUITE)
        worker_report = dict(WORKER, window=dict(runtime_seconds=43_200))
        # generating and clearing that took most of a run: more samples, and a longer phase, than
        # the earlier journal could hold
        summary = raw["system_telemetry"]
        summary["phases"][-1].update(elapsed_seconds=40_000.5, samples=4_500)
        summary["samples"] = 20 * (len(summary["phases"]) - 1) + 4_500
        self.assertGreater(summary["samples"], 4_096)
        extra = diagnostics.derive(record, b"archive", raw, worker_report)
        self.assertEqual((extra["system"]["status"], extra["system"]["samples"]), ("complete", summary["samples"]))
        self.assertEqual((extra["system"]["phases"][-1]["elapsed_seconds"], extra["system"]["phases"][-1]["samples"]),
                         (40_000.5, 4_500))
        diagnostics.validate(extra, record)
        # a summary beyond what any journal can hold is not believed
        for field, phase, value in (("samples", None, 5_121), ("samples", -1, 5_121), ("elapsed_seconds", -1, 43_200.5)):
            beyond = json.loads(json.dumps(raw))
            target = beyond["system_telemetry"] if phase is None else beyond["system_telemetry"]["phases"][phase]
            target[field] = value
            with self.subTest(field=field, phase=phase):
                self.assertEqual(diagnostics.derive(record, b"archive", beyond, worker_report)["system"],
                                 dict(status="unavailable", samples=0, phases=[]))

    def test_a_run_sampled_under_the_earlier_bounds_is_published_as_before(self):
        raw, _ = sampled_matrix(SUITE, diagnostics.TELEMETRY_DESCRIPTOR)
        record, _ = matrix_example(SUITE)
        extra = diagnostics.derive(record, b"archive", raw, WORKER)
        self.assertEqual(extra["setup"]["instrumentation"], telemetry.descriptor())
        self.assertEqual(extra["limits"]["allocation_seconds"], 10_800)
        diagnostics.validate(extra, record)
        # bounds that no runner has ever had are not a known contract: nothing sampled is published
        raw["context"]["system_telemetry"] = dict(telemetry.descriptor(), max_seconds=24 * 3600)
        unknown = diagnostics.derive(record, b"archive", raw, WORKER)
        self.assertIsNone(unknown["setup"]["instrumentation"])
        self.assertTrue(all(trial["system"]["status"] == "unavailable" for trial in unknown["trials"]))


if __name__ == "__main__":
    unittest.main()
