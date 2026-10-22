import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest

from .common import BINARIES, MAX_BINARY
from .production_policy import Policy, WorkflowRun, authorize, manifest, schedule_slot

NOW = datetime(2026, 10, 22, 20, 0, tzinfo=timezone.utc)
SHA = "a" * 40


def identity(**updates):
    values = dict(repository="wykurz/rcp", repository_id="123", repository_owner_id="483465",
                  actor_id="483465", actor="wykurz", ref="refs/heads/main",
                  workflow_ref="wykurz/rcp/.github/workflows/aws-production.yml@refs/heads/main",
                  workflow_sha=SHA, sha=SHA, run_id="100", run_attempt="1", event_name="workflow_dispatch")
    return SimpleNamespace(**(values | updates))


def policy(**updates):
    values = dict(version="policy-1", owner_id="483465", enabled=True, total_cap_microusd=350_000_000,
                  monthly_cap_microusd=80_000_000, daily_run_limit=1, hourly_microusd=4_608_000,
                  ancillary_microusd=1_000_000, priced_at=NOW.isoformat(), price_evidence="test price receipt")
    return Policy(**(values | updates))


def inputs(**updates):
    return dict(baseline=SHA, candidate=SHA, suite="bulk-1g-metadata",
                binaries={key: dict(sha256="b" * 64, bytes=100) for key in BINARIES}) | updates


def run_json(who=None, **updates):
    who = who or identity()
    return dict(id=int(who.run_id), run_attempt=int(who.run_attempt),
                repository=dict(id=int(who.repository_id), full_name=who.repository,
                                owner=dict(id=int(who.repository_owner_id))),
                head_sha=who.sha, head_branch="main", event=who.event_name,
                actor=dict(id=int(who.actor_id)), path=".github/workflows/aws-production.yml",
                status="in_progress", conclusion=None, created_at=(NOW - timedelta(minutes=30)).isoformat()) | updates


class ProductionPolicyTests(unittest.TestCase):
    def authorize(self, configured=None, who=None, value=None, run=None, now=NOW):
        who = who or identity()
        run = run or WorkflowRun.verify(run_json(who), who, NOW)
        return authorize(configured or policy(), who, run, value or inputs(), now)

    def test_default_policy_is_inert(self):
        with self.assertRaisesRegex(ValueError, "disabled"):
            self.authorize(Policy(version="disabled", owner_id="483465"))

    def test_full_runtime_shutdown_and_ancillary_are_reserved(self):
        result = self.authorize()
        self.assertEqual(result["reserve_microusd"], 16_052_800)
        self.assertEqual(result["run_key"], "repo-123-run-100")
        self.assertEqual(result["month"], "2026-10")
        self.assertEqual(result["day"], "2026-10-22")
        self.assertIsNone(result["schedule_slot"])

    def test_price_rounds_up_sub_micro_dollars(self):
        self.assertEqual(policy(hourly_microusd=1).reservation(NOW), 1_000_004)

    def test_owner_only_until_maintainer_ids_explicitly_selected(self):
        with self.assertRaisesRegex(ValueError, "not approved"):
            self.authorize(who=identity(actor_id="55"))
        self.authorize(policy(manual_actor_ids=("55",)), identity(actor_id="55"))

    def test_other_repository_owner_cannot_reuse_policy(self):
        with self.assertRaisesRegex(ValueError, "policy owner"):
            self.authorize(who=identity(repository_owner_id="55"))

    def test_event_names_do_not_grant_paid_admission(self):
        for event in ("pull_request", "pull_request_target", "push", "issue_comment", "workflow_run"):
            with self.subTest(event=event), self.assertRaises(ValueError):
                self.authorize(who=identity(event_name=event))

    def test_run_key_and_epoch_survive_rerun(self):
        first, second = self.authorize(), self.authorize(who=identity(run_attempt="2"))
        self.assertEqual(first["run_key"], second["run_key"])
        self.assertEqual(first["epoch"], second["epoch"])
        self.assertNotEqual(first["fingerprint"], second["fingerprint"])

    def test_new_token_does_not_refresh_old_run_creation_time(self):
        with self.assertRaisesRegex(ValueError, "stale"):
            WorkflowRun.verify(run_json(created_at=(NOW - timedelta(hours=3)).isoformat()), identity(), NOW)

    def test_stale_run_read_must_be_refreshed(self):
        with self.assertRaisesRegex(ValueError, "observation is stale"):
            self.authorize(now=NOW + timedelta(seconds=61))

    def test_exact_run_producer_and_active_state_are_required(self):
        changes = [dict(id=101), dict(run_attempt=2), dict(head_sha="c" * 40), dict(head_branch="debug"),
                   dict(event="push"), dict(actor={"id": 99}), dict(path=".github/workflows/other.yml"),
                   dict(status="completed", conclusion="success"), dict(conclusion="cancelled"),
                   dict(repository=dict(id=123, full_name="other/rcp", owner=dict(id=483465)))]
        for update in changes:
            with self.subTest(update=update), self.assertRaises(ValueError):
                WorkflowRun.verify(run_json(**update), identity(), NOW)

    def test_fresh_run_proof_cannot_be_rebound_to_another_signed_producer(self):
        proof = WorkflowRun.verify(run_json(), identity(), NOW)
        for update in (dict(run_id="101"), dict(run_attempt="2"), dict(actor_id="55"),
                       dict(repository_id="99"), dict(event_name="schedule")):
            with self.subTest(update=update), self.assertRaises(ValueError):
                self.authorize(who=identity(**update), run=proof)

    def test_future_run_and_price_or_stale_price_fail_closed(self):
        with self.assertRaises(ValueError):
            WorkflowRun.verify(run_json(created_at=(NOW + timedelta(seconds=1)).isoformat()), identity(), NOW)
        for at in (NOW + timedelta(seconds=1), NOW - timedelta(days=32)):
            with self.subTest(at=at), self.assertRaises(ValueError):
                self.authorize(policy(priced_at=at.isoformat()))

    def test_caller_quote_and_extra_manifest_fields_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "fields"):
            self.authorize(value=inputs(cap_usd=999999))

    def test_unfunded_or_zero_server_limits_do_not_admit(self):
        for update in (dict(total_cap_microusd=1), dict(monthly_cap_microusd=1),
                       dict(daily_run_limit=0), dict(hourly_microusd=0), dict(ancillary_microusd=0)):
            with self.subTest(update=update), self.assertRaises(ValueError):
                self.authorize(policy(**update))

    def test_invalid_types_and_weakened_runtime_margins_rejected(self):
        for update in (dict(total_cap_microusd=True), dict(hourly_microusd=4.608),
                       dict(enabled="true"), dict(shutdown_seconds=10), dict(runtime_seconds=3600),
                       dict(capacity_seconds=43201), dict(daily_run_limit=100),
                       dict(schedule_slots=((0, 24, 0),))):
            with self.subTest(update=update), self.assertRaises(ValueError):
                policy(**update)

    def test_manifest_requires_exact_bounded_identical_aa_binaries(self):
        changed = inputs()
        changed["binaries"]["candidate/rcp"]["sha256"] = "d" * 64
        too_large = inputs()
        too_large["binaries"]["candidate/rcp"]["bytes"] = MAX_BINARY + 1
        wrong_type = inputs()
        wrong_type["binaries"]["candidate/rcp"]["bytes"] = True
        for value in (changed, too_large, wrong_type, inputs(candidate="c" * 40), inputs(candidate="main"),
                      inputs(suite="arbitrary")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                manifest(value)

    def test_no_approved_schedule_means_no_scheduled_launch(self):
        with self.assertRaisesRegex(ValueError, "schedule slot"):
            self.authorize(who=identity(event_name="schedule"))

    def test_schedule_uses_server_slot_and_pinned_revisions(self):
        configured = policy(schedule_slots=((3, 19, 30),), scheduled_baseline=SHA)
        result = self.authorize(configured, identity(event_name="schedule"))
        self.assertEqual(result["schedule_slot"], "2026-10-22T19:30Z")
        with self.assertRaises(ValueError):
            self.authorize(replace(configured, scheduled_baseline="d" * 40), identity(event_name="schedule"))

    def test_unapproved_times_and_ambiguous_slots_fail_closed(self):
        for slots in (((3, 19, 31),), ((3, 18, 0),), ((3, 19, 0), (3, 19, 30))):
            with self.subTest(slots=slots), self.assertRaises(ValueError):
                self.authorize(policy(schedule_slots=slots, scheduled_baseline=SHA), identity(event_name="schedule"))

    def test_schedule_handles_week_boundary_without_caller_slot(self):
        created = datetime(2026, 10, 26, 0, 10, tzinfo=timezone.utc)
        configured = policy(schedule_slots=((6, 23, 50),), scheduled_baseline=SHA)
        self.assertEqual(schedule_slot(configured, created, created), "2026-10-25T23:50Z")

    def test_validated_manifest_is_not_alias_of_caller_data(self):
        value = inputs()
        result = self.authorize(value=value)
        value["binaries"]["candidate/rcp"]["sha256"] = "d" * 64
        self.assertEqual(result["manifest"]["binaries"]["candidate/rcp"]["sha256"], "b" * 64)


if __name__ == "__main__":
    unittest.main()
