"""Offline proposal CLI and captured-settings checks; not live IAM acceptance."""

from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from . import production_trust as trust

ROOT = Path(__file__).resolve().parents[2]
SECRET = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/rcp/production/github-read-app-Ab1Cd2"
ROLES = ["arn:aws:iam::123456789012:role/production/" + name for name in ("admission", "collector", "recovery")]


def readbacks(plan):
    environment = dict(name=trust.ENVIRONMENT, url=trust.ENVIRONMENT_URL, can_admins_bypass=False,
        deployment_branch_policy=deepcopy(plan["environment"]["settings"]["deployment_branch_policy"]),
        protection_rules=[dict(type="required_reviewers", prevent_self_review=False,
                              reviewers=[dict(type="User", reviewer=dict(id=trust.OWNER_ID))])])
    branches = dict(endpoint=trust.ENVIRONMENT_URL + "/deployment-branch-policies",
                    response=dict(total_count=1, branch_policies=[dict(id=123, name="main", type="branch")]))
    rules = [dict(id=100 + i, source=trust.REPOSITORY, source_type="Repository", **deepcopy(value)) for i, value in enumerate(plan["rulesets"])]
    return environment, branches, rules


class TrustTests(unittest.TestCase):
    def setUp(self):
        self.plan = trust.writer_plan("1234", "Iv23.test-client", "5678")

    def test_split_rulesets_never_let_writer_bypass_deletion_or_force_push(self):
        update, preserve = self.plan["rulesets"]
        self.assertEqual(update["bypass_actors"], [dict(actor_id=1234, actor_type="Integration", bypass_mode="always")])
        self.assertEqual(update["rules"], [dict(type="creation"), dict(type="update", parameters=dict(update_allows_fetch_and_merge=False))])
        self.assertEqual(preserve["rules"], [dict(type="deletion"), dict(type="non_fast_forward")])
        self.assertEqual(preserve["bypass_actors"], [])
        for rule in (update, preserve):
            self.assertEqual(rule["conditions"], dict(ref_name=dict(include=[trust.HISTORY_REF], exclude=[])))
            self.assertEqual(rule["enforcement"], "active")
        self.assertEqual(self.plan["variables"]["RCP_AWS_HISTORY_TRUST_READY"], "false")
        self.assertEqual(self.plan["variables"]["RCP_AWS_PRODUCTION_PUBLISH_ENABLED"], "false")
        self.assertEqual(self.plan["app"]["permissions"], dict(contents="write", metadata="read"))

    def test_owner_can_approve_only_selected_main_environment(self):
        settings = self.plan["environment"]["settings"]
        self.assertEqual(settings, dict(wait_timer=0, prevent_self_review=False,
            reviewers=[dict(type="User", id=trust.OWNER_ID)],
            deployment_branch_policy=dict(protected_branches=False, custom_branch_policies=True)))
        self.assertNotIn("can_admins_bypass", settings)
        self.assertEqual(self.plan["environment"]["branch_policy"], dict(name="main", type="branch"))
        self.assertEqual(self.plan["secret"]["scope"], "environment")

    def test_complete_readbacks_match_but_do_not_claim_live_enforcement(self):
        env, branches, rules = readbacks(self.plan)
        rules[0]["rules"].reverse()
        value = trust.check_writer(self.plan, env, branches, list(reversed(rules)))
        self.assertTrue(value["settings_match"])
        self.assertFalse(value["live_enforcement_proven"])

    def test_missing_or_broadened_environment_never_passes_as_exact_main(self):
        variants = []
        for value in (True, None, 0, "false"):
            variants.append(lambda env, branches, rules, value=value: env.update(can_admins_bypass=value))
        variants.extend([
            lambda env, branches, rules: env.pop("can_admins_bypass"),
            lambda env, branches, rules: env.update(url=trust.ENVIRONMENT_URL.replace("wykurz", "other")),
            lambda env, branches, rules: branches.update(endpoint=trust.ENVIRONMENT_URL + "/other"),
            lambda env, branches, rules: env.update(deployment_branch_policy=None),
            lambda env, branches, rules: env["deployment_branch_policy"].update(protected_branches=True),
            lambda env, branches, rules: env["protection_rules"][0].update(prevent_self_review=True),
            lambda env, branches, rules: env["protection_rules"][0]["reviewers"][0]["reviewer"].update(id=1),
            lambda env, branches, rules: env["protection_rules"].clear(),
            lambda env, branches, rules: branches["response"]["branch_policies"][0].update(name="*"),
            lambda env, branches, rules: branches["response"]["branch_policies"][0].update(type="tag"),
            lambda env, branches, rules: branches["response"].update(total_count=2),
        ])
        for mutate in variants:
            env, branches, rules = readbacks(self.plan)
            mutate(env, branches, rules)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                trust.check_writer(self.plan, env, branches, rules)

    def test_missing_bypass_is_not_no_bypass_and_unrelated_actors_are_rejected(self):
        variants = [
            lambda rules: rules[1].pop("bypass_actors"),
            lambda rules: rules[1].update(source="other/rcp"),
            lambda rules: rules[1].pop("source_type"),
            lambda rules: rules[1].update(bypass_actors=deepcopy(rules[0]["bypass_actors"])),
            lambda rules: rules[0]["bypass_actors"][0].update(actor_id=5678),
            lambda rules: rules[0]["bypass_actors"].append(dict(actor_id=5, actor_type="RepositoryRole", bypass_mode="always")),
            lambda rules: rules[0]["conditions"]["ref_name"].update(include=["~ALL"]),
            lambda rules: rules[0]["rules"][1]["parameters"].update(update_allows_fetch_and_merge=True),
            lambda rules: rules[1]["rules"].pop(),
            lambda rules: rules[1].update(enforcement="disabled"),
            lambda rules: rules[1].update(id=100),
        ]
        for mutate in variants:
            env, branches, rules = readbacks(self.plan)
            mutate(rules)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                trust.check_writer(self.plan, env, branches, rules)

    def test_secret_policy_adds_only_deny_for_exact_resource_and_selected_roles(self):
        policy = trust.reader_policy(SECRET, *ROLES)
        self.assertEqual(policy["Version"], "2012-10-17")
        outside, stages = policy["Statement"]
        for statement in policy["Statement"]:
            self.assertEqual(statement["Effect"], "Deny")
            self.assertEqual(statement["Action"], "secretsmanager:GetSecretValue")
            self.assertEqual(statement["Resource"], SECRET)
            self.assertEqual(statement["Principal"], "*")
        self.assertEqual(outside["Condition"], {"ArnNotEquals": {"aws:PrincipalArn": ROLES}})
        self.assertEqual(stages["Condition"], {"ArnEquals": {"aws:PrincipalArn": ROLES[:2]},
            "StringNotEquals": {"secretsmanager:VersionStage": "AWSCURRENT"}})
        # this models only these deny predicates, never a claim of effective IAM permission
        def denied(principal, stage=None):
            return principal not in ROLES or (principal in ROLES[:2] and stage != "AWSCURRENT")
        for principal in ROLES[:2]:
            self.assertFalse(denied(principal, "AWSCURRENT"))
            for stage in (None, "AWSPREVIOUS", "", "other"):
                self.assertTrue(denied(principal, stage))
        self.assertFalse(denied(ROLES[2]))
        for principal in (None, "arn:aws:iam::123456789012:root", ROLES[0] + "-other"):
            self.assertTrue(denied(principal, "AWSCURRENT"))

    def test_secret_requires_explicit_distinct_same_account_roles_without_wildcards(self):
        for value in ("", ROLES[0], ROLES[2].replace("123456789012", "999999999999"),
                      "arn:aws:sts::123456789012:assumed-role/recovery/session", ROLES[2] + "*", "<recovery-role>"):
            with self.subTest(role=value), self.assertRaises(ValueError):
                trust.reader_policy(SECRET, ROLES[0], ROLES[1], value)
        for value in (SECRET + "*", SECRET.replace("github-read-app", "other"), "<secret-arn>"):
            with self.subTest(secret=value), self.assertRaises(ValueError):
                trust.reader_policy(value, *ROLES)

    def cli(self, *args):
        return subprocess.run([sys.executable, "-m", "infra.aws_benchmark.production_trust", *args],
            cwd=ROOT, env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"), capture_output=True, text=True, timeout=10)

    def test_real_cli_rejects_key_material_without_echoing_it(self):
        for value in ("PRIVATE_MATERIAL_MUST_NOT_APPEAR", "-----BEGIN PRIVATE KEY-----\nSYNTHETIC_CANARY\n-----END PRIVATE KEY-----", SECRET + "\nSYNTHETIC_CANARY"):
            with self.subTest(kind="invalid resource identifier"):
                result = self.cli("reader-policy", "--secret-arn=" + value, "--admission-role", ROLES[0],
                                  "--collector-role", ROLES[1], "--recovery-role", ROLES[2])
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn(value, result.stdout + result.stderr)
                self.assertNotIn("SYNTHETIC_CANARY", result.stdout + result.stderr)
                self.assertEqual(result.stdout.strip(), "Trust proposal rejected: invalid or incomplete non-secret inputs.")

    def test_real_cli_renders_without_credentials_and_requires_missing_identities(self):
        args = ("--app-id", "1234", "--client-id", "Iv23.test-client", "--installation-id", "5678")
        result = self.cli("writer-plan", *args)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), self.plan)
        self.assertNotEqual(self.cli("writer-plan", *args[:-2]).returncode, 0)
        self.assertNotEqual(self.cli("writer-plan", *args[:-1], "wrong").returncode, 0)
        result = self.cli("reader-policy", "--secret-arn", SECRET, "--admission-role", ROLES[0],
                          "--collector-role", ROLES[1], "--recovery-role", ROLES[2])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), trust.reader_policy(SECRET, *ROLES))
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for name, value in zip(("environment", "branches", "rulesets"), readbacks(self.plan)):
                path = Path(directory) / (name + ".json")
                path.write_text(json.dumps(value))
                paths.extend(["--" + name + "-readback", str(path)])
            result = self.cli("writer-check", *args, *paths)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(json.loads(result.stdout)["live_enforcement_proven"])
            (Path(directory) / "rulesets.json").write_text("[]")
            self.assertNotEqual(self.cli("writer-check", *args, *paths).returncode, 0)


if __name__ == "__main__":
    unittest.main()
