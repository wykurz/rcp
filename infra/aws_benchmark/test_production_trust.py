"""Offline metadata/CLI contracts; never live IAM or key-custody acceptance.

The static fixture uses synthetic App/ruleset IDs and GitHub REST response shapes,
including bare update rules independently observed on the repository's ruleset.
It is intentionally not built from writer_plan().
"""

from copy import deepcopy
import fnmatch
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from . import production_trust as trust
from .reader_identity import reader_resource_identity

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = Path(__file__).with_name("fixtures") / "history-trust-readbacks-v2.json"
RESOURCE = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/rcp/production/github-read-app-Ab1Cd2"
ROLES = ["arn:aws:iam::123456789012:role/production/" + name for name in ("admission", "collector", "recovery")]
STACK = "arn:aws:cloudformation:us-east-1:123456789012:stack/rcp-production-test/12345678-1234-1234-1234-123456789abc"
FIELDS = ("environment", "branches", "rulesets", "repository", "app", "inventory")
ARGS = ("--app-id", "1234", "--client-id", "Iv23.test-client", "--installation-id", "5678")


def readbacks():
    return json.loads(FIXTURE.read_text())


def page_response(inventory):
    return inventory["pages"][0]["response"]


def stack_readback():
    return dict(operation="DescribeStacks", region="us-east-1", parameters=dict(StackName=STACK),
        response=dict(Stacks=[dict(StackId=STACK, StackName="rcp-production-test", StackStatus="CREATE_COMPLETE",
            Parameters=[dict(ParameterKey=key, ParameterValue="false") for key in
                        ("RuntimeEnabled", "AdmissionEnabled", "PublicationEnabled")],
            Outputs=[dict(OutputKey=key, OutputValue=value) for key, value in
                     zip(("AdmissionRoleArn", "CollectorRoleArn"), ROLES[:2])])]))


def rendered_deny(policy, resource, principal, stage=None, action="secretsmanager:GetSecretValue"):
    """Evaluate only the rendered subset; not an effective-permission simulator."""
    def values(value):
        return value if isinstance(value, list) else [value]
    for statement in policy["Statement"]:
        assert statement["Effect"] == "Deny" and statement["Principal"] == "*"
        if not any(fnmatch.fnmatchcase(action, x) for x in values(statement["Action"])):
            continue
        if not any(fnmatch.fnmatchcase(resource, x) for x in values(statement["Resource"])):
            continue
        conditions = []
        for operator, terms in statement["Condition"].items():
            assert operator in {"ArnEquals", "ArnNotEquals", "StringNotEquals"}
            for key, expected in terms.items():
                assert key in {"aws:PrincipalArn", "secretsmanager:VersionStage"}
                actual = principal if key == "aws:PrincipalArn" else stage
                equals = actual in values(expected)
                conditions.append(equals if operator == "ArnEquals" else not equals)
        if all(conditions):
            return True
    return False


class TrustTests(unittest.TestCase):
    def setUp(self):
        self.plan = trust.writer_plan("1234", "Iv23.test-client", "5678")

    def check(self, data):
        return trust.check_writer(self.plan, *(data[key] for key in FIELDS))

    def test_observed_github_shape_and_scoped_disabled_plan(self):
        data = readbacks()
        self.assertEqual(data["rulesets"][0]["rules"][1], {"type": "update"})
        result = self.check(data)
        self.assertTrue(result["settings_match"])
        self.assertTrue(result["configured_secret_name_placement_verified"])
        self.assertTrue(result["app_metadata_identity_verified"])
        for key in ("live_enforcement_proven", "key_custody_proven", "installation_scope_proven"):
            self.assertFalse(result[key])
        self.assertNotIn("installation_id", result)
        self.assertEqual(self.plan["variables"]["repository"], {key: "false" for key in trust.REPOSITORY_FLAGS})
        self.assertEqual(self.plan["variables"]["environment"], {
            trust.CLIENT_VARIABLE: "Iv23.test-client", trust.INSTALLATION_VARIABLE: "5678"})
        for rule in self.plan["rulesets"]:
            self.assertEqual(rule["conditions"]["ref_name"]["include"], [trust.HISTORY_REF, trust.HISTORY_NAMESPACE])
        self.assertEqual(self.plan["rulesets"][1]["bypass_actors"], [])

    def test_before_key_phase_checks_protection_without_first_importing_key(self):
        data = readbacks()
        page_response(data["inventory"]["environment_secrets"][trust.ENVIRONMENT]).update(total_count=0, secrets=[])
        result = trust.check_writer(self.plan, *(data[key] for key in FIELDS), key_state="absent")
        self.assertEqual(result["key_metadata_state"], "absent")
        self.assertFalse(result["key_custody_proven"])
        with self.assertRaises(ValueError): self.check(data)
        with self.assertRaises(ValueError):
            trust.check_writer(self.plan, *(readbacks()[key] for key in FIELDS), key_state="absent")
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for name, value in data.items():
                path = Path(directory) / (name + ".json"); path.write_text(json.dumps(value))
                paths.extend(["--" + name + "-readback", path])
            result = self.cli("writer-check", *ARGS, *paths, "--key-state", "absent")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["key_metadata_state"], "absent")

    def test_bare_or_explicit_false_only_with_nonfork_repository(self):
        data = readbacks()
        data["rulesets"][0]["rules"][1]["parameters"] = {"update_allows_fetch_and_merge": False}
        self.check(data)
        for value in (True, 0, None, "false"):
            data["rulesets"][0]["rules"][1]["parameters"]["update_allows_fetch_and_merge"] = value
            with self.subTest(value=value), self.assertRaises(ValueError): self.check(data)
        data = readbacks()
        data["repository"]["response"]["fork"] = True
        with self.assertRaisesRegex(ValueError, "non-fork"): self.check(data)

    def test_environment_and_rule_boundaries_reject_missing_typed_or_broadened_evidence(self):
        variants = [
            lambda d: d["environment"].pop("can_admins_bypass"),
            lambda d: d["environment"].update(can_admins_bypass=True),
            lambda d: d["environment"].update(url=trust.ENVIRONMENT_URL.replace("wykurz", "other")),
            lambda d: d["environment"]["protection_rules"][0].update(prevent_self_review=True),
            lambda d: d["environment"]["protection_rules"][0]["reviewers"][0]["reviewer"].update(id=True),
            lambda d: d["environment"]["protection_rules"].append({"type": "wait_timer", "wait_timer": False}),
            lambda d: d["environment"]["protection_rules"].append(deepcopy(d["environment"]["protection_rules"][0])),
            lambda d: d["branches"].update(endpoint="https://api.github.com/other"),
            lambda d: d["branches"]["response"].update(total_count=True),
            lambda d: d["branches"]["response"].update(total_count=1.0),
            lambda d: d["branches"]["response"]["branch_policies"][0].update(name="*"),
            lambda d: d["rulesets"][0]["conditions"]["ref_name"]["include"].pop(),
            lambda d: d["rulesets"][0]["bypass_actors"][0].update(actor_id=15368),
            lambda d: d["rulesets"][1].pop("bypass_actors"),
            lambda d: d["rulesets"][1].update(bypass_actors=deepcopy(d["rulesets"][0]["bypass_actors"])),
            lambda d: d["rulesets"][1].update(source="other/rcp"),
            lambda d: d["rulesets"][1].update(id=True),
            lambda d: d["rulesets"][1].update(id=100),
            lambda d: d["rulesets"][1].update(enforcement="disabled"),
        ]
        for mutate in variants:
            data = readbacks(); mutate(data)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError): self.check(data)

    def test_app_identity_is_bound_to_repository_owner_numeric_id_client_and_permissions(self):
        variants = [
            lambda d: d["app"].update(endpoint="https://api.github.com/apps/wrong"),
            lambda d: d["app"]["response"].update(id=True),
            lambda d: d["app"]["response"].update(id=15368),
            lambda d: d["app"]["response"].update(client_id="Iv23.other"),
            lambda d: d["app"]["response"]["owner"].update(id=123),
            lambda d: d["app"]["response"]["permissions"].update(actions="write"),
            lambda d: d["app"]["response"].update(events=["push"]),
            lambda d: d["repository"]["response"].update(id=655073755.0),
            lambda d: d["repository"]["response"]["owner"].update(type="Organization"),
        ]
        for mutate in variants:
            data = readbacks(); mutate(data)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError): self.check(data)

    def test_complete_inventory_prevents_secret_scope_and_variable_shadowing_mistakes(self):
        variants = [
            lambda i: i["environment_secrets"].pop("other"),
            lambda i: i["environment_variables"].pop("other"),
            lambda i: i["repository_secrets"]["pages"][0].update(endpoint=trust.API_ROOT + "/dependabot/secrets?per_page=100&page=1"),
            lambda i: page_response(i["repository_secrets"]).update(total_count=True),
            lambda i: page_response(i["repository_secrets"]).update(total_count=0.0),
            lambda i: page_response(i["environments"])["environments"][0].update(url="https://api.github.com/other"),
            lambda i: page_response(i["environment_secrets"][trust.ENVIRONMENT])["secrets"][0].update(value="synthetic"),
            lambda i: page_response(i["repository_secrets"]).update(total_count=1, secrets=[{"name": trust.WRITER_SECRET_NAME.lower()}]),
        ]
        for target in ("repository_secrets", "dependabot_secrets"):
            variants.append(lambda i, target=target: page_response(i[target]).update(total_count=1, secrets=[{"name": trust.WRITER_SECRET_NAME}]))
        variants.extend([
            lambda i: page_response(i["environment_secrets"]["other"]).update(total_count=1, secrets=[{"name": trust.WRITER_SECRET_NAME}]),
            lambda i: page_response(i["environment_secrets"][trust.ENVIRONMENT]).update(total_count=0, secrets=[]),
            lambda i: page_response(i["environment_variables"][trust.ENVIRONMENT])["variables"][0].update(value="Iv23.other"),
            lambda i: page_response(i["environment_variables"]["other"]).update(total_count=1, variables=[{"name": trust.REPOSITORY_FLAGS[0], "value": "false"}]),
            lambda i: page_response(i["repository_variables"])["variables"][0].update(value="true"),
            lambda i: page_response(i["repository_variables"])["variables"].append({"name": trust.CLIENT_VARIABLE, "value": "Iv23.test-client"}),
        ])
        for mutate in variants:
            data = readbacks(); mutate(data["inventory"])
            with self.subTest(mutation=mutate), self.assertRaises(ValueError): self.check(data)

    def test_inventory_checks_every_page_and_rejects_duplicates(self):
        endpoint = trust.API_ROOT + "/actions/secrets"
        names = [{"name": "NAME_" + str(n)} for n in range(101)]
        pages = {"pages": [{"endpoint": endpoint + "?per_page=100&page=" + str(n),
            "response": {"total_count": 101, "secrets": names[(n - 1) * 100:n * 100]}} for n in (1, 2)]}
        self.assertEqual(len(trust.inventory_items(pages, endpoint, "secrets")), 101)
        for mutate in (
            lambda p: p["pages"].pop(),
            lambda p: p["pages"][1].update(endpoint=endpoint + "?per_page=100&page=1"),
            lambda p: p["pages"][1]["response"].update(total_count=102),
            lambda p: p["pages"][1]["response"]["secrets"][0].update(name="NAME_0"),
        ):
            value = deepcopy(pages); mutate(value)
            with self.assertRaises(ValueError): trust.inventory_items(value, endpoint, "secrets")

    def test_rendered_deny_covers_replica_arn_and_stage_context_without_adding_allows(self):
        policy = trust.reader_policy(RESOURCE, *ROLES)
        for resource in (RESOURCE, RESOURCE.replace("us-east-1", "us-west-2")):
            for role in ROLES[:2]:
                self.assertFalse(rendered_deny(policy, resource, role, "AWSCURRENT"))
                for stage in (None, "AWSPREVIOUS", "", "other"):
                    self.assertTrue(rendered_deny(policy, resource, role, stage))
            self.assertFalse(rendered_deny(policy, resource, ROLES[2]))
            for role in (None, ROLES[0] + "-other", "arn:aws:iam::123456789012:root"):
                self.assertTrue(rendered_deny(policy, resource, role, "AWSCURRENT"))
        self.assertTrue(all(s["Resource"] == "*" for s in policy["Statement"]))
        self.assertFalse(rendered_deny(policy, RESOURCE, "other", action="secretsmanager:DescribeSecret"))

    def test_reader_resource_grammar_matches_runtime_and_roles_do_not_broaden(self):
        variants = [RESOURCE + "*", RESOURCE.replace("github-read-app", "other"),
            RESOURCE.replace("arn:aws:", "arn:aws-us-gov:").replace("us-east-1", "us-gov-east-1"),
            RESOURCE.replace("arn:aws:", "arn:aws-cn:").replace("us-east-1", "cn-north-1"), "<resource-arn>"]
        self.assertEqual(reader_resource_identity(RESOURCE), ("us-east-1", "123456789012"))
        for value in variants:
            self.assertIsNone(reader_resource_identity(value))
            with self.assertRaises(ValueError): trust.reader_policy(value, *ROLES)
        for role in ("", ROLES[0], ROLES[2] + "*", ROLES[2].replace("123456789012", "999999999999"),
                     "arn:aws:sts::123456789012:assumed-role/recovery/session"):
            with self.assertRaises(ValueError): trust.reader_policy(RESOURCE, ROLES[0], ROLES[1], role)

    def test_reader_proposal_binds_exact_disabled_stack_outputs_and_attachment(self):
        result = trust.reader_proposal(RESOURCE, STACK, stack_readback(), ROLES[2])
        self.assertEqual(result["reader_roles"], ROLES[:2])
        self.assertEqual(result["attachment"], dict(secret_id=RESOURCE, block_public_policy=True))
        self.assertEqual(result["resource_policy"], trust.reader_policy(RESOURCE, *ROLES))
        mutations = [
            lambda d: d.update(region="us-west-2"),
            lambda d: d["parameters"].update(StackName="other"),
            lambda d: d["response"]["Stacks"][0].update(StackId=STACK + "other"),
            lambda d: d["response"]["Stacks"][0].update(StackStatus="UPDATE_IN_PROGRESS"),
            lambda d: d["response"]["Stacks"][0]["Parameters"][0].update(ParameterValue="true"),
            lambda d: d["response"]["Stacks"][0]["Outputs"].pop(),
            lambda d: d["response"]["Stacks"][0]["Outputs"].append(deepcopy(d["response"]["Stacks"][0]["Outputs"][0])),
            lambda d: d["response"]["Stacks"][0]["Outputs"][0].update(OutputValue=ROLES[2]),
        ]
        for mutate in mutations:
            value = stack_readback(); mutate(value)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                trust.reader_proposal(RESOURCE, STACK, value, ROLES[2])

    def cli(self, *args):
        return subprocess.run([sys.executable, "-B", "-m", "infra.aws_benchmark.production_trust", *map(str, args)],
            cwd=ROOT, env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"), capture_output=True, text=True, timeout=20)

    def test_cli_outputs_only_valid_json_and_uses_no_clobber_file_output(self):
        result = self.cli("writer-plan", *ARGS)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), self.plan)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "proposal.json"
            result = self.cli("writer-plan", *ARGS, "--output", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "")
            self.assertEqual(json.loads(output.read_text()), self.plan)
            before = output.read_bytes()
            result = self.cli("writer-plan", *ARGS, "--output", output)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("output already exists", result.stderr)
            self.assertEqual(output.read_bytes(), before)
            link = Path(directory) / "symlink.json"; link.symlink_to(output)
            result = self.cli("writer-plan", *ARGS, "--output", link)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(output.read_bytes(), before)
            self.assertEqual(list(Path(directory).glob(".trust-proposal-*")), [])

    def test_invalid_cli_never_echoes_arguments_or_writes_output(self):
        marker = "SYNTHETIC_PRIVATE_CANARY"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "proposal.json"
            for args in ((marker,), ("writer-plan", *ARGS, marker),
                         ("writer-plan", *ARGS, "--private-key", marker),
                         ("writer-plan", *ARGS[:-1], marker)):
                result = self.cli(*args, "--output", output)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertNotIn(marker, result.stderr)
                self.assertNotIn("Traceback", result.stderr)
                self.assertFalse(output.exists())

    def test_cli_checks_all_evidence_and_gives_sanitized_actionable_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for name, value in readbacks().items():
                path = Path(directory) / (name + ".json"); path.write_text(json.dumps(value))
                paths.extend(["--" + name + "-readback", path])
            result = self.cli("writer-check", *ARGS, *paths)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(json.loads(result.stdout)["key_custody_proven"])
            env = readbacks()["environment"]; env["can_admins_bypass"] = True
            (Path(directory) / "environment.json").write_text(json.dumps(env))
            result = self.cli("writer-check", *ARGS, *paths)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")
            self.assertIn("administrator bypass disabled is unproven", result.stderr)

    def test_cli_reader_derives_roles_from_stack_instead_of_unchecked_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stack.json"; path.write_text(json.dumps(stack_readback()))
            args = ("reader-policy", "--secret-arn", RESOURCE, "--stack-id", STACK,
                    "--stack-readback", path, "--recovery-role", ROLES[2])
            result = self.cli(*args)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["reader_roles"], ROLES[:2])
            result = self.cli(*args, "--admission-role", ROLES[2])
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(ROLES[2], result.stderr)

    def test_strict_readback_rejects_duplicates_nan_deep_json_links_and_special_files(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            for raw in ('{"a":1,"a":2}', '{"a":NaN}', '[' * 100000, '"' + 'x' * (1024 * 1024) + '"'):
                path.write_text(raw)
                with self.assertRaises(ValueError): trust.read(path)
            path.write_text('{}')
            link = Path(directory) / "link.json"; link.symlink_to(path)
            with self.assertRaises(OSError): trust.read(link)
            hard = Path(directory) / "hard.json"; os.link(path, hard)
            with self.assertRaises(ValueError): trust.read(path)
            fifo = Path(directory) / "fifo"; os.mkfifo(fifo)
            with self.assertRaises(ValueError): trust.read(fifo)

    def test_unexpected_readback_failure_has_no_traceback_or_raw_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stack.json"; path.write_text('[' * 100000)
            result = self.cli("reader-policy", "--secret-arn", RESOURCE, "--stack-id", STACK,
                              "--stack-readback", path, "--recovery-role", ROLES[2])
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")
            self.assertNotIn("Traceback", result.stderr)
            self.assertIn("JSON nesting exceeds limit", result.stderr)

    def test_file_publish_failure_does_not_replace_or_leave_temporary_material(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "out.json"
            with patch.object(trust.os, "link", side_effect=OSError("synthetic")):
                with self.assertRaises(OSError): trust.emit({"ok": True}, path)
            self.assertFalse(path.exists())
            self.assertEqual(list(Path(directory).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
