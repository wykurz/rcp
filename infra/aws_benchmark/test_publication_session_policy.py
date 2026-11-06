"""Offline IAM-subset contract checks, not an AWS authorization simulator or live proof.

This evaluator accepts only the grammar actually used here, fails on other operators,
and distinguishes explicit from implicit denial. AWS references and live gates are in
ACCEPTANCE.md. In particular, conditions AND keys, and negated IfExists denies absent
keys. It deliberately does not evaluate the role, key, table or organization policies.
"""

import fnmatch
import hashlib
import itertools
import json
import unittest

from . import acceptance_publication as probe


KEY = "arn:aws:kms:us-east-1:381183037896:key/fda6ab01-08d2-49d9-9866-f75012f06ed6"
CONTEXT = {
    "kms:ViaService": "dynamodb.us-east-1.amazonaws.com",
    "kms:CallerAccount": "381183037896",
    "kms:EncryptionContext:aws:dynamodb:tableName": "rcp-production-benchmark-LeaseTable-137SHCUGC8HHU",
    "kms:EncryptionContext:aws:dynamodb:subscriberId": "381183037896",
}


def evaluate(policy, action, resource, context):
    """Evaluate this session document alone, including IAM's deny precedence."""
    def matches(patterns, value, *, action=False):
        values = [patterns] if isinstance(patterns, str) else patterns
        return any(fnmatch.fnmatchcase(value.lower(), p.lower()) if action
            else fnmatch.fnmatchcase(value, p) for p in values)
    effects = set()
    for statement in policy["Statement"]:
        if (set(statement) - {"Effect", "Action", "NotAction", "Resource", "NotResource", "Condition"}
                or ("Action" in statement) == ("NotAction" in statement)
                or ("Resource" in statement) == ("NotResource" in statement)
                or statement.get("Effect") not in {"Allow", "Deny"}):
            raise AssertionError("unsupported statement grammar")
        if ("Action" in statement and not matches(statement["Action"], action, action=True)
                or "NotAction" in statement and matches(statement["NotAction"], action, action=True)
                or "Resource" in statement and not matches(statement["Resource"], resource)
                or "NotResource" in statement and matches(statement["NotResource"], resource)):
            continue
        conditions = []
        for operator, pairs in statement.get("Condition", {}).items():
            for key, expected in pairs.items():
                if operator == "StringEquals":
                    conditions.append(key in context and context[key] == expected)
                elif operator == "StringNotEqualsIfExists":
                    conditions.append(key not in context or context[key] != expected)
                else:
                    raise AssertionError("unsupported condition operator")
        if all(conditions):
            effects.add(statement["Effect"])
    return "explicit-deny" if "Deny" in effects else "allow" if "Allow" in effects else "implicit-deny"


class PublicationSessionPolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = json.loads(probe.SESSION_POLICY)

    def decision(self, *, action="kms:Decrypt", resource=KEY, context=None):
        return evaluate(self.policy, action, resource, CONTEXT if context is None else context)

    def test_exact_delegation_is_eligible_within_sts_plaintext_limit(self):
        self.assertEqual(self.decision(), "allow")
        self.assertEqual(probe.TABLE_KEY_ARN, KEY)
        self.assertEqual(probe.KMS_CONTEXT, CONTEXT)
        self.assertEqual(len(self.policy["Statement"]), 8)
        self.assertEqual(self.policy["Statement"][2], dict(Effect="Allow", Action="kms:Decrypt",
            Resource=KEY, Condition={"StringEquals": CONTEXT}))
        wire = probe.SESSION_POLICY.encode("ascii")
        self.assertLessEqual(len(wire), 2048)
        self.assertEqual(probe.SESSION_POLICY_SHA256, hashlib.sha256(wire).hexdigest())

    def test_direct_kms_even_with_spoofable_encryption_context_is_explicitly_denied(self):
        for context in ({}, {k: v for k, v in CONTEXT.items() if k != "kms:ViaService"}):
            with self.subTest(context=context):
                self.assertEqual(self.decision(context=context), "explicit-deny")

    def test_each_and_every_combination_of_missing_or_mismatched_conditions_is_denied(self):
        keys = list(CONTEXT)
        # A single combined deny would AND mismatches and miss single-field failures.
        for states in itertools.product(("exact", "missing", "wrong"), repeat=len(keys)):
            context = {k: CONTEXT[k] if state == "exact" else "mismatch" for k, state in zip(keys, states)
                if state != "missing"}
            with self.subTest(states=states):
                expected = "allow" if all(s == "exact" for s in states) else "explicit-deny"
                self.assertEqual(self.decision(context=context), expected)

    def test_wrong_service_region_account_table_and_subscriber_are_separately_denied(self):
        for key, values in {
            "kms:ViaService": ["s3.us-east-1.amazonaws.com", "dynamodb.us-west-2.amazonaws.com", "dynamodb.*.amazonaws.com"],
            "kms:CallerAccount": ["381183037897"],
            "kms:EncryptionContext:aws:dynamodb:tableName": [probe.TABLE + "-other"],
            "kms:EncryptionContext:aws:dynamodb:subscriberId": ["381183037897"],
        }.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    self.assertEqual(self.decision(context={**CONTEXT, key: value}), "explicit-deny")

    def test_other_keys_are_explicitly_denied_even_with_exact_delegation(self):
        for resource in (KEY[:-1] + "7", KEY.replace("us-east-1", "us-west-2"),
                KEY.replace("381183037896", "381183037897"), "arn:aws:kms:us-east-1:381183037896:alias/aws/dynamodb"):
            with self.subTest(resource=resource):
                self.assertEqual(self.decision(resource=resource), "explicit-deny")

    def test_two_data_reads_stay_eligible_on_arbitrary_resources_contexts_and_both_fixture_keys(self):
        # This session must not synthesize the negative resource/LeadingKeys denials.
        resources = [("dynamodb:GetItem", "arn:aws:dynamodb:us-east-1:381183037896:table/" + name)
            for name in (probe.TABLE, "another")]
        resources += [("s3:GetObjectVersion", "arn:aws:s3:::" + probe.BUCKET + "/" + key)
            for key in (probe.MANIFEST, probe.FORBIDDEN)]
        for action, resource in resources:
            for context in ({}, {"dynamodb:LeadingKeys": "policy"}, {"dynamodb:LeadingKeys": "run#" + probe.RUN}):
                with self.subTest(action=action, resource=resource, context=context):
                    self.assertEqual(self.decision(action=action, resource=resource, context=context), "allow")

    def test_other_kms_actions_and_all_mutating_privileges_remain_explicitly_denied(self):
        for action in ("kms:Encrypt", "kms:GenerateDataKey", "kms:CreateGrant", "kms:DescribeKey", "kms:ReEncryptFrom",
                "dynamodb:PutItem", "dynamodb:Scan", "s3:PutObject", "s3:GetObject", "s3:ListBucket",
                "ec2:RunInstances", "lambda:InvokeFunction", "secretsmanager:GetSecretValue", "iam:PassRole"):
            with self.subTest(action=action):
                self.assertEqual(self.decision(action=action), "explicit-deny")

    def test_evaluator_does_not_claim_live_authorization_or_accept_unknown_operators(self):
        self.assertIn("live-kms-direct-and-context-denials", probe.UNPROVEN)
        altered = {"Statement": [{"Effect": "Allow", "Action": "kms:Decrypt", "Resource": KEY,
            "Condition": {"Unsupported": {"x": "y"}}}]}
        with self.assertRaises(AssertionError):
            evaluate(altered, "kms:Decrypt", KEY, CONTEXT)

    def test_evaluator_matches_action_names_without_case_and_rejects_unknown_statement_fields(self):
        self.assertEqual(self.decision(action="kms:decrypt"), "allow")
        self.assertEqual(self.decision(action="DynamoDB:getitem", context={}), "allow")
        self.assertEqual(self.decision(resource=KEY.upper()), "explicit-deny")
        altered = {"Statement": [{"Effect": "Allow", "Action": "kms:Decrypt", "Resource": KEY,
            "Unsupported": "must not ignore"}]}
        with self.assertRaises(AssertionError):
            evaluate(altered, "kms:Decrypt", KEY, CONTEXT)
