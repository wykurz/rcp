"""Offline permission and inert-default checks for the separate production stack.

The small evaluator supports only the operators this template uses. These checks
are not AWS policy simulation or deployed integration evidence.
"""

from copy import deepcopy
import fnmatch
import json
from pathlib import Path
import re
import unittest

import yaml

from infra.aws_benchmark.production_store import RunStore


ROOT = Path(__file__).resolve().parent
TEMPLATE = yaml.safe_load((ROOT / "production-stack.yaml").read_text())
LEGACY = yaml.safe_load((ROOT / "durable-stack.yaml").read_text())
PREFIX = "rcp-production-benchmark"
ACCOUNT = "123456789012"
REGION = "us-east-1"
BUCKET = "production-artifacts"
BUCKET_ARN = "arn:aws:s3:::" + BUCKET
TABLE = "arn:aws:dynamodb:us-east-1:123456789012:table/production"
MACHINE = "arn:aws:states:us-east-1:123456789012:stateMachine:" + PREFIX + "-capacity"
INSTANCE = "arn:aws:ec2:us-east-1:123456789012:instance/i-00000000000000001"
OTHER_INSTANCE = INSTANCE[:-1] + "2"
RUN = "repo-655073755-run-1234"
RUN_PK = "run#" + RUN
BINARY_NAMES = [variant + "/" + name for variant in ("baseline", "candidate")
                for name in ("rcp", "rcpd", "filegen")]
VALUES = {
    "AWS::Partition": "aws", "AWS::Region": REGION, "AWS::AccountId": ACCOUNT,
    "ResourcePrefix": PREFIX, "ArtifactBucket": BUCKET, "ArtifactBucket.Arn": BUCKET_ARN,
    "RuntimeScheduleGroup": PREFIX + "-runtime", "LeaseTable": "production", "LeaseTable.Arn": TABLE,
    "LaunchTemplateId": "lt-00000000000000001", "LaunchTemplateVersion": "7",
    "WorkerAmiId": "ami-00000000000000001", "WorkerSubnetId": "subnet-00000000000000001",
    "WorkerSecurityGroupId": "sg-00000000000000001", "WorkerSourceRevision": "a" * 40,
    "WorkerInstanceProfile.Arn": "arn:aws:iam::123456789012:instance-profile/" + PREFIX + "-worker",
    "WorkerRole.Arn": "arn:aws:iam::123456789012:role/" + PREFIX + "-worker",
    "SchedulerTerminationRole.Arn": "arn:aws:iam::123456789012:role/production-termination",
    "CoordinatorStateMachine": MACHINE, "CoordinatorStateMachine.Name": PREFIX + "-capacity",
    "AdmissionAudience": "rcp-production-benchmark-admission",
    "GitHubOidcProviderArn": "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com",
    **{name + "Function.Arn": "arn:aws:lambda:us-east-1:123456789012:function:"
       + PREFIX + "-" + name.lower() for name in ("Admission", "Coordinator", "Watchdog", "Collector")},
}


def sequence(value):
    return value if isinstance(value, list) else [value]


def resolve(value, enabled=True, admission=True, publication=True):
    if isinstance(value, list):
        return [resolve(item, enabled, admission, publication) for item in value]
    if not isinstance(value, dict):
        return value
    if set(value) == {"Ref"}:
        return VALUES[value["Ref"]]
    if set(value) == {"Fn::GetAtt"}:
        return VALUES[".".join(value["Fn::GetAtt"])]
    if set(value) == {"Fn::If"}:
        condition, yes, no = value["Fn::If"]
        assert condition in {"Enabled", "Admitting", "Publishing"}
        active = {"Enabled": enabled, "Admitting": enabled and admission, "Publishing": publication}[condition]
        return resolve(yes if active else no, enabled, admission, publication)
    if set(value) == {"Fn::Sub"}:
        def replace(match):
            name = match[1]
            return "${" + name[1:] + "}" if name.startswith("!") else str(VALUES[name])
        return re.sub(r"\$\{([^}]+)\}", replace, value["Fn::Sub"])
    return {key: resolve(item, enabled, admission, publication) for key, item in value.items()}


def statements(role, enabled=True, admission=True, publication=True):
    return [resolve(statement, enabled, admission, publication)
            for policy in TEMPLATE["Resources"][role]["Properties"]["Policies"]
            for statement in policy["PolicyDocument"]["Statement"]]


def conditions_match(conditions, context):
    for operator, terms in conditions.items():
        for key, expected in terms.items():
            if operator == "Null":
                if (key not in context) != (expected == "true"):
                    return False
            elif operator == "NumericEquals":
                if key not in context or float(context[key]) != float(expected):
                    return False
            elif operator in {"StringEquals", "ArnEquals"}:
                if context.get(key) not in sequence(expected):
                    return False
            elif operator in {"StringLike", "ArnLike"}:
                if key not in context or not any(fnmatch.fnmatchcase(str(context[key]), str(item))
                                                 for item in sequence(expected)):
                    return False
            elif operator == "Bool":
                if key not in context or str(context[key]).lower() != str(expected).lower():
                    return False
            elif operator == "ForAllValues:StringLike":
                if not all(any(fnmatch.fnmatchcase(item, pattern) for pattern in sequence(expected))
                           for item in sequence(context.get(key, []))):
                    return False
            elif operator == "ForAllValues:StringEquals":
                if not all(item in sequence(expected) for item in sequence(context.get(key, []))):
                    return False
            elif operator == "ForAnyValue:StringEquals":
                if not any(item in sequence(expected) for item in sequence(context.get(key, []))):
                    return False
            else:
                raise AssertionError("unsupported policy operator: " + operator)
    return True


def permits(role, action, resource, context=None, *, enabled=True, admission=True, publication=True):
    context = context or {}
    allowed = False
    for statement in statements(role, enabled, admission, publication):
        if not any(fnmatch.fnmatchcase(action, item) for item in sequence(statement["Action"])):
            continue
        patterns = [re.sub(r"\$\{([^}]+)\}", lambda match: str(context.get(match[1], "<missing>")), item)
                    for item in sequence(statement["Resource"])]
        if not any(fnmatch.fnmatchcase(resource, item) for item in patterns):
            continue
        if not conditions_match(statement.get("Condition", {}), context):
            continue
        if statement["Effect"] == "Deny":
            return False
        allowed |= statement["Effect"] == "Allow"
    return allowed


def db_context(*keys, transaction=True):
    value = {"dynamodb:LeadingKeys": list(keys)}
    if transaction:
        value["dynamodb:EnclosingOperation"] = ["TransactWriteItems"]
    return value


def update_context(update, *, transaction=True):
    # derive accessed attributes from the actual held request, not a parallel IAM-only shape
    names = set(update["Key"]) | set(update["ExpressionAttributeNames"].values())
    return db_context(update["Key"]["pk"]["S"], transaction=transaction) | {"dynamodb:Attributes": sorted(names)}


def progress_update():
    return RunStore(None, "production", RUN, "a" * 64)._state_update({"request": {}, "phase": "armed"}, 0)["Update"]


def instance_key(tail, instance=INSTANCE):
    return BUCKET_ARN + "/production/instances/" + instance + "/" + tail


def launch_context():
    tags = {"rcp:namespace": PREFIX, "rcp:run": "1234-1", "rcp:attempt": "1234-1-1",
            "rcp:deadline": "2026-10-22T03:00:00Z", "rcp:created-at": "2026-10-22T00:00:00Z"}
    return {"ec2:LaunchTemplate": "arn:aws:ec2:us-east-1:123456789012:launch-template/" + VALUES["LaunchTemplateId"],
            "ec2:InstanceProfile": VALUES["WorkerInstanceProfile.Arn"], "ec2:InstanceType": "c5d.24xlarge",
            "ec2:InstanceMarketType": "on-demand", "ec2:Tenancy": "default", "ec2:MetadataHttpTokens": "required",
            "ec2:MetadataHttpEndpoint": "enabled", "ec2:MetadataHttpPutResponseHopLimit": 1,
            "ec2:InstanceMetadataTags": "disabled", "ec2:VolumeType": "gp3", "ec2:VolumeSize": 16,
            "ec2:Encrypted": "true", "ec2:Subnet": "arn:aws:ec2:us-east-1:123456789012:subnet/" + VALUES["WorkerSubnetId"],
            "aws:TagKeys": list(tags), **{"aws:RequestTag/" + key: value for key, value in tags.items()}}


class ProductionStackTests(unittest.TestCase):
    def test_default_flags_disable_every_invocation_and_background_tick(self):
        self.assertEqual(TEMPLATE["Parameters"]["ResourcePrefix"]["Default"], PREFIX)
        for name in ("RuntimeEnabled", "AdmissionEnabled", "PublicationEnabled"):
            self.assertEqual(TEMPLATE["Parameters"][name]["Default"], "false")
        for enabled, admitting in ((False, False), (False, True), (True, False), (True, True)):
            for name in ("Admission", "Coordinator", "Watchdog", "Collector"):
                props = TEMPLATE["Resources"][name + "Function"]["Properties"]
                actual = resolve(props["ReservedConcurrentExecutions"], enabled, admitting)
                self.assertEqual(actual, int(enabled and (admitting or name != "Admission")))
            self.assertEqual(permits("GitHubSubmitRole", "lambda:InvokeFunction", VALUES["AdmissionFunction.Arn"],
                             enabled=enabled, admission=admitting), enabled and admitting)
        rule = TEMPLATE["Resources"]["WatchdogRule"]["Properties"]
        self.assertEqual(resolve(rule["State"], False), "DISABLED")
        self.assertEqual(resolve(rule["State"], True, False), "ENABLED")
        self.assertEqual(rule["Targets"][0]["RetryPolicy"], {"MaximumEventAgeInSeconds": 60, "MaximumRetryAttempts": 0})

    def test_github_trust_binds_native_main_identity_and_only_invokes_admission(self):
        props = TEMPLATE["Resources"]["GitHubSubmitRole"]["Properties"]
        trust = props["AssumeRolePolicyDocument"]["Statement"]
        self.assertEqual(len(trust), 1)
        conditions = trust[0]["Condition"]
        expected = {"aud": "sts.amazonaws.com", "sub": "repo:wykurz/rcp:ref:refs/heads/main",
                    "repository": "wykurz/rcp", "repository_id": "655073755", "repository_owner_id": "483465",
                    "actor_id": "483465", "ref": "refs/heads/main", "workflow": "AWS production benchmark"}
        context = {"token.actions.githubusercontent.com:" + key: value for key, value in expected.items()}
        self.assertEqual(conditions, {"StringEquals": context})
        self.assertTrue(conditions_match(conditions, context))
        for key in context:
            for bad in (None, "another", "refs/pull/1/merge", "*"):
                changed = dict(context)
                if bad is None:
                    del changed[key]
                else:
                    changed[key] = bad
                self.assertFalse(conditions_match(conditions, changed), (key, bad))
        self.assertEqual(props["MaxSessionDuration"], 3600)
        self.assertEqual(statements("GitHubSubmitRole"), [{"Effect": "Allow", "Action": "lambda:InvokeFunction",
                         "Resource": VALUES["AdmissionFunction.Arn"]}])
        for action, resource in (("lambda:InvokeFunction", VALUES["CoordinatorFunction.Arn"]),
                                 ("lambda:InvokeFunction", VALUES["WatchdogFunction.Arn"]),
                                 ("states:StartExecution", MACHINE), ("dynamodb:UpdateItem", TABLE),
                                 ("iam:PassRole", VALUES["WorkerRole.Arn"]), ("s3:GetObject", instance_key("output/status.json")),
                                 ("ec2:RunInstances", INSTANCE), ("ssm:SendCommand", "*")):
            self.assertFalse(permits("GitHubSubmitRole", action, resource), action)

    def test_admission_environment_uses_submit_only_signed_audience_and_pins(self):
        env = TEMPLATE["Resources"]["AdmissionFunction"]["Properties"]["Environment"]["Variables"]
        values = resolve(env)
        self.assertEqual(values["RCP_PRODUCTION_SUBMIT_WORKFLOW"],
                         "wykurz/rcp/.github/workflows/aws-benchmark-production.yml@refs/heads/main")
        self.assertEqual(values["RCP_PRODUCTION_AUDIENCE"], "rcp-production-benchmark-admission")
        self.assertEqual(values["RCP_PRODUCTION_REPOSITORY_ID"], "655073755")
        self.assertEqual(values["RCP_PRODUCTION_OWNER_ID"], "483465")
        self.assertEqual(values["RCP_PRODUCTION_MACHINE_ARN"], MACHINE)
        self.assertEqual(values["RCP_DURABLE_PROFILE_ARN"], VALUES["WorkerInstanceProfile.Arn"])
        self.assertEqual(values["RCP_DURABLE_BUCKET"], BUCKET)
        self.assertEqual(values["RCP_DURABLE_TABLE"], "production")
        for enabled, admitting in ((False, True), (True, False)):
            self.assertEqual(resolve(env, enabled, admitting)["RCP_PRODUCTION_ADMISSION_ENABLED"], "false")
        self.assertNotIn("collect", str(env).lower())

    def test_admission_reservation_writes_are_transaction_only_and_policy_is_read_only(self):
        for action, keys in (("PutItem", ["active", RUN_PK, "schedule#2026-10-22T03:00Z"]),
                            ("ConditionCheckItem", ["policy", "active"])):
            for key in keys:
                self.assertTrue(permits("AdmissionRole", "dynamodb:" + action, TABLE, db_context(key)), (action, key))
                self.assertFalse(permits("AdmissionRole", "dynamodb:" + action, TABLE,
                                        db_context(key, transaction=False)), (action, key))
        self.assertTrue(permits("AdmissionRole", "dynamodb:GetItem", TABLE, db_context("policy", transaction=False)))
        for action in ("PutItem", "UpdateItem", "DeleteItem"):
            self.assertFalse(permits("AdmissionRole", "dynamodb:" + action, TABLE, db_context("policy")))
        for key in ("active", RUN_PK, "budget#total", "schedule#slot"):
            self.assertFalse(permits("AdmissionRole", "dynamodb:DeleteItem", TABLE, db_context(key)))
        for key in ("guard", "run#repo-999-run-1234", "unrelated"):
            self.assertFalse(permits("AdmissionRole", "dynamodb:UpdateItem", TABLE, db_context(key)))
        self.assertFalse(permits("AdmissionRole", "dynamodb:UpdateItem", TABLE, db_context(RUN_PK, "policy")))
        self.assertFalse(permits("AdmissionRole", "dynamodb:UpdateItem", TABLE, {}))
        self.assertFalse(permits("AdmissionRole", "dynamodb:UpdateItem", TABLE + "-other", db_context(RUN_PK)))

    def test_runtime_cannot_reserve_refund_seed_policy_or_release_owner(self):
        for role in ("CoordinatorRole", "WatchdogRole"):
            self.assertTrue(permits(role, "dynamodb:GetItem", TABLE, db_context("policy", "active", RUN_PK)))
            self.assertTrue(permits(role, "dynamodb:UpdateItem", TABLE, update_context(progress_update())))
            self.assertFalse(permits(role, "dynamodb:UpdateItem", TABLE, db_context(RUN_PK, transaction=False)))
            for key in ("policy", "active", "budget#total", "budget#month#2026-10", "rate#day#2026-10-22", "schedule#slot"):
                for action in ("UpdateItem", "PutItem", "DeleteItem"):
                    self.assertFalse(permits(role, "dynamodb:" + action, TABLE, db_context(key)), (role, action, key))
            self.assertFalse(permits(role, "dynamodb:DeleteItem", TABLE, db_context(RUN_PK)))
            self.assertFalse(permits(role, "dynamodb:UpdateItem", TABLE, db_context("run#repo-999-run-1234")))
        self.assertTrue(permits("WatchdogRole", "dynamodb:PutItem", TABLE, db_context("guard", transaction=False)))
        self.assertFalse(permits("CoordinatorRole", "dynamodb:PutItem", TABLE, db_context("guard")))
        self.assertTrue(permits("CoordinatorRole", "dynamodb:ConditionCheckItem", TABLE, db_context("policy")))

    def test_run_progress_permissions_match_sdk_request_and_reject_immutable_fields(self):
        update = progress_update()
        context = update_context(update)
        self.assertEqual(set(context["dynamodb:Attributes"]), {"pk", "epoch", "revision", "closed", "terminal", "state"})
        for role in ("AdmissionRole", "CoordinatorRole", "WatchdogRole"):
            self.assertTrue(permits(role, "dynamodb:UpdateItem", TABLE, context), role)
            self.assertFalse(permits(role, "dynamodb:UpdateItem", TABLE, update_context(update, transaction=False)), role)
            self.assertFalse(permits(role, "dynamodb:UpdateItem", TABLE, db_context(RUN_PK)), role)
            self.assertFalse(permits(role, "dynamodb:UpdateItem", TABLE,
                                     {"dynamodb:Attributes": context["dynamodb:Attributes"],
                                      "dynamodb:EnclosingOperation": ["TransactWriteItems"]}), role)
            for forbidden in ("admission", "fingerprint", "retirement_sha256", "reserved", "runs"):
                for attributes in (["pk", forbidden], context["dynamodb:Attributes"] + [forbidden]):
                    self.assertFalse(permits(role, "dynamodb:UpdateItem", TABLE,
                        context | {"dynamodb:Attributes": attributes}), (role, forbidden))
        self.assertTrue(permits("CoordinatorRole", "dynamodb:ConditionCheckItem", TABLE, db_context("policy")))
        self.assertFalse(permits("CoordinatorRole", "dynamodb:ConditionCheckItem", TABLE,
                                db_context("policy", transaction=False)))

    def test_admission_counter_attributes_are_separate_from_progress_and_each_other(self):
        store = RunStore(None, "production", RUN, "a" * 64)
        counters = [("budget#total", "reserved", True), ("budget#month#2026-10", "reserved", False),
                    ("rate#day#2026-10-22", "runs", False)]
        for key, field, existing in counters:
            update = store._counter_update(key, field, 1, 10, existing=existing)["Update"]
            context = update_context(update)
            self.assertEqual(context["dynamodb:Attributes"], ["pk", field])
            self.assertTrue(permits("AdmissionRole", "dynamodb:UpdateItem", TABLE, context), key)
            self.assertFalse(permits("AdmissionRole", "dynamodb:UpdateItem", TABLE,
                                    update_context(update, transaction=False)), key)
            self.assertFalse(permits("AdmissionRole", "dynamodb:UpdateItem", TABLE, db_context(key)), key)
            for forbidden in ("state", "admission", "fingerprint", "runs" if field == "reserved" else "reserved"):
                self.assertFalse(permits("AdmissionRole", "dynamodb:UpdateItem", TABLE,
                    context | {"dynamodb:Attributes": ["pk", field, forbidden]}), (key, forbidden))
            for role in ("CoordinatorRole", "WatchdogRole", "CollectorRole"):
                self.assertFalse(permits(role, "dynamodb:UpdateItem", TABLE, context), (role, key))
        self.assertFalse(permits("AdmissionRole", "dynamodb:UpdateItem", TABLE,
            db_context("budget#total", RUN_PK) | {"dynamodb:Attributes": ["pk", "reserved", "state"]}))

    def test_worker_reads_only_its_credential_origin_and_writes_only_create_only_outputs(self):
        context = {"ec2:SourceInstanceArn": INSTANCE, "s3:if-none-match": "*", "s3:x-amz-server-side-encryption": "AES256"}
        for key in ["input/" + name for name in BINARY_NAMES] + ["input/request.json", "control/window.json"]:
            self.assertTrue(permits("WorkerRole", "s3:GetObject", instance_key(key), context), key)
            self.assertFalse(permits("WorkerRole", "s3:PutObject", instance_key(key), context), key)
            self.assertFalse(permits("WorkerRole", "s3:GetObject", instance_key(key, OTHER_INSTANCE), context), key)
        for key in ("output/status.json", "output/results.zip"):
            self.assertTrue(permits("WorkerRole", "s3:PutObject", instance_key(key), context))
            self.assertFalse(permits("WorkerRole", "s3:PutObject", instance_key(key, OTHER_INSTANCE), context))
            for field in context:
                bad = dict(context)
                del bad[field]
                self.assertFalse(permits("WorkerRole", "s3:PutObject", instance_key(key), bad), field)
            bad = context | {"s3:if-none-match": "etag"}
            self.assertFalse(permits("WorkerRole", "s3:PutObject", instance_key(key), bad))
            bad = context | {"ec2:SourceInstanceArn": INSTANCE.replace(ACCOUNT, "999999999999")}
            self.assertFalse(permits("WorkerRole", "s3:PutObject", instance_key(key, bad["ec2:SourceInstanceArn"]), bad))
            for action in ("s3:DeleteObject", "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:PutObjectAcl"):
                self.assertFalse(permits("WorkerRole", action, instance_key(key), context))
        for key in ("output/status.json", "production/staging/" + RUN + "/baseline/rcp",
                    "production/runs/" + RUN + "/durable-receipt.json"):
            self.assertFalse(permits("WorkerRole", "s3:PutObject", BUCKET_ARN + "/" + key, context))
        for action in ("s3:ListBucket", "s3:ListBucketVersions", "s3:PutBucketPolicy", "s3:PutLifecycleConfiguration"):
            self.assertFalse(permits("WorkerRole", action, BUCKET_ARN, context))

    def test_staging_capabilities_are_exact_binary_keys_with_no_overwrite_or_delete(self):
        condition = {"s3:if-none-match": "*", "s3:x-amz-server-side-encryption": "AES256"}
        for binary in BINARY_NAMES:
            key = BUCKET_ARN + "/production/staging/" + RUN + "/" + binary
            self.assertTrue(permits("AdmissionRole", "s3:PutObject", key, condition))
            self.assertTrue(permits("AdmissionRole", "s3:GetObject", key))
            self.assertFalse(permits("AdmissionRole", "s3:PutObject", key, {}))
            for role in ("CoordinatorRole", "WatchdogRole"):
                self.assertTrue(permits(role, "s3:GetObjectVersion", key))
                self.assertFalse(permits(role, "s3:PutObject", key, condition))
            for action in ("s3:DeleteObject", "s3:DeleteObjectVersion"):
                self.assertFalse(permits("AdmissionRole", action, key))
        for tail in ("request.json", "baseline/extra", "baseline/rcp/extra"):
            self.assertFalse(permits("AdmissionRole", "s3:PutObject", BUCKET_ARN + "/production/staging/" + RUN + "/" + tail, condition))
        other = BUCKET_ARN + "/production/staging/repo-999-run-1234/baseline/rcp"
        self.assertFalse(permits("AdmissionRole", "s3:PutObject", other, condition))

    def test_watchdog_can_publish_instance_mailbox_but_never_worker_results(self):
        condition = {"s3:if-none-match": "*", "s3:x-amz-server-side-encryption": "AES256"}
        for key in ["input/" + name for name in BINARY_NAMES] + ["input/request.json", "control/window.json"]:
            self.assertTrue(permits("WatchdogRole", "s3:GetObject", instance_key(key)))
            self.assertTrue(permits("WatchdogRole", "s3:PutObject", instance_key(key), condition))
            self.assertFalse(permits("WatchdogRole", "s3:PutObject", instance_key(key), {}))
        for role in ("AdmissionRole", "CoordinatorRole", "WatchdogRole"):
            for tail in ("output/status.json", "output/results.zip"):
                self.assertFalse(permits(role, "s3:PutObject", instance_key(tail), condition))
        self.assertTrue(permits("WatchdogRole", "s3:PutObject",
                              BUCKET_ARN + "/production/runs/" + RUN + "/durable-receipt.json", condition))
        self.assertFalse(permits("WatchdogRole", "s3:PutObject", BUCKET_ARN + "/production/runs/" + RUN + "/collection/evidence.json", condition))

    def test_watchdog_list_is_limited_to_one_exact_status_key(self):
        prefix = "production/instances/" + INSTANCE + "/output/status.json"
        context = {"s3:prefix": prefix, "s3:max-keys": 1}
        self.assertTrue(permits("WatchdogRole", "s3:ListBucket", BUCKET_ARN, context))
        for changed in ({}, {"s3:prefix": prefix}, context | {"s3:max-keys": 2},
                        context | {"s3:prefix": "production/instances/"},
                        context | {"s3:prefix": prefix.replace(ACCOUNT, "999999999999")},
                        context | {"s3:prefix": prefix.replace("status.json", "results.zip")}):
            self.assertFalse(permits("WatchdogRole", "s3:ListBucket", BUCKET_ARN, changed), changed)
        for role in ("AdmissionRole", "CoordinatorRole", "GitHubSubmitRole", "WorkerRole"):
            self.assertFalse(permits(role, "s3:ListBucket", BUCKET_ARN, context), role)

    def test_launch_policy_preserves_reviewed_direct_launch_constraints(self):
        current = TEMPLATE["Resources"]["CoordinatorRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        baseline = LEGACY["Resources"]["CoordinatorRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        expected = deepcopy([s for s in baseline if s["Action"] in ("ec2:RunInstances", "ec2:CreateTags", "iam:PassRole")])
        for statement in expected:
            if statement["Action"] == "iam:PassRole":
                statement["Resource"] = {"Fn::GetAtt": ["WorkerRole", "Arn"]}
            equals = statement.get("Condition", {}).get("ArnEquals", {})
            if "ec2:InstanceProfile" in equals:
                equals["ec2:InstanceProfile"] = {"Fn::GetAtt": ["WorkerInstanceProfile", "Arn"]}
        actual = [s for s in current if s["Action"] in ("ec2:RunInstances", "ec2:CreateTags", "iam:PassRole")]
        self.assertEqual(actual, expected)
        context = launch_context()
        self.assertTrue(permits("CoordinatorRole", "ec2:RunInstances", INSTANCE, context))
        for key, bad in {"ec2:LaunchTemplate": "other", "ec2:InstanceProfile": "other", "ec2:InstanceType": "c5d.metal",
                         "ec2:InstanceMarketType": "spot", "ec2:Tenancy": "dedicated", "ec2:MetadataHttpTokens": "optional",
                         "ec2:MetadataHttpPutResponseHopLimit": 2, "ec2:MetadataHttpEndpoint": "disabled",
                         "ec2:InstanceMetadataTags": "enabled", "aws:RequestTag/rcp:namespace": "legacy",
                         "aws:TagKeys": list(context["aws:TagKeys"]) + ["arbitrary"]}.items():
            self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances", INSTANCE, context | {key: bad}), key)
        for key in ("rcp:run", "rcp:attempt", "rcp:deadline", "rcp:created-at"):
            changed = dict(context)
            del changed["aws:RequestTag/" + key]
            self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances", INSTANCE, changed), key)
        volume = "arn:aws:ec2:us-east-1:123456789012:volume/vol-00000000000000001"
        self.assertTrue(permits("CoordinatorRole", "ec2:RunInstances", volume, context))
        for key, bad in (("ec2:VolumeSize", 100), ("ec2:VolumeType", "io2"), ("ec2:Encrypted", "false")):
            self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances", volume, context | {key: bad}))
        self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances", OTHER_INSTANCE.replace(ACCOUNT, "999999999999"), context))

    def test_termination_and_passrole_are_confined_to_reviewed_destinations(self):
        tags = {"ec2:ResourceTag/rcp:namespace": PREFIX}
        for role in ("WatchdogRole", "SchedulerTerminationRole"):
            self.assertTrue(permits(role, "ec2:TerminateInstances", INSTANCE, tags))
            for context in ({}, {"ec2:ResourceTag/rcp:namespace": "rcp-durable-benchmark"}):
                self.assertFalse(permits(role, "ec2:TerminateInstances", INSTANCE, context))
            self.assertFalse(permits(role, "ec2:TerminateInstances", INSTANCE.replace(ACCOUNT, "999999999999"), tags))
        self.assertTrue(permits("CoordinatorRole", "iam:PassRole", VALUES["WorkerRole.Arn"], {"iam:PassedToService": "ec2.amazonaws.com"}))
        self.assertFalse(permits("CoordinatorRole", "iam:PassRole", VALUES["WorkerRole.Arn"], {"iam:PassedToService": "lambda.amazonaws.com"}))
        self.assertFalse(permits("CoordinatorRole", "iam:PassRole", VALUES["WorkerRole.Arn"] + "-other", {"iam:PassedToService": "ec2.amazonaws.com"}))
        self.assertTrue(permits("WatchdogRole", "iam:PassRole", VALUES["SchedulerTerminationRole.Arn"], {"iam:PassedToService": "scheduler.amazonaws.com"}))
        for role in ("AdmissionRole", "GitHubSubmitRole", "WorkerRole"):
            self.assertFalse(permits(role, "iam:PassRole", VALUES["WorkerRole.Arn"], {"iam:PassedToService": "ec2.amazonaws.com"}))

    def test_standard_machine_binds_repository_run_epoch_and_execution_identity(self):
        machine = TEMPLATE["Resources"]["CoordinatorStateMachine"]["Properties"]
        self.assertEqual(machine["StateMachineType"], "STANDARD")
        self.assertEqual(machine["LoggingConfiguration"], {"Level": "OFF", "IncludeExecutionData": False})
        self.assertEqual(machine["TracingConfiguration"], {"Enabled": False})
        observe = machine["Definition"]["States"]["Observe"]
        self.assertEqual(observe["Parameters"], {"run_key.$": "$.run_key", "epoch.$": "$.epoch", "run.$": "$.run",
                         "execution_arn.$": "$$.Execution.Id", "execution_name.$": "$$.Execution.Name"})
        self.assertEqual(observe["Retry"][0]["MaxAttempts"], 6)
        self.assertNotIn("States.ALL", observe["Retry"][0]["ErrorEquals"])
        self.assertEqual(observe["Catch"][0]["Next"], "Failed")
        for action in ("states:StartExecution", "states:DescribeStateMachine"):
            self.assertTrue(permits("AdmissionRole", action, MACHINE))
            self.assertFalse(permits("AdmissionRole", action, MACHINE + "-other"))
        execution = MACHINE.replace(":stateMachine:", ":execution:") + ":1234-1"
        self.assertTrue(permits("AdmissionRole", "states:DescribeExecution", execution))
        self.assertFalse(permits("AdmissionRole", "states:StopExecution", execution))
        self.assertFalse(permits("AdmissionRole", "states:StartSyncExecution", MACHINE))
        self.assertFalse(permits("WatchdogRole", "states:StartExecution", MACHINE))

    def test_storage_is_new_private_versioned_and_retention_is_prefix_specific(self):
        bucket = TEMPLATE["Resources"]["ArtifactBucket"]["Properties"]
        self.assertEqual(bucket["VersioningConfiguration"], {"Status": "Enabled"})
        self.assertTrue(all(bucket["PublicAccessBlockConfiguration"].values()))
        self.assertEqual(bucket["OwnershipControls"], {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]})
        self.assertEqual(bucket["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]["ServerSideEncryptionByDefault"], {"SSEAlgorithm": "AES256"})
        all_rules = bucket["LifecycleConfiguration"]["Rules"]
        rules = [rule for rule in all_rules if "ExpirationInDays" in rule]
        markers = [rule for rule in all_rules if rule.get("ExpiredObjectDeleteMarker")]
        self.assertEqual(len(markers), 1)
        self.assertEqual(markers[0]["Prefix"], "production/")
        self.assertNotIn("ExpirationInDays", markers[0])
        self.assertEqual({rule["Prefix"]: rule["ExpirationInDays"] for rule in rules},
                         {"production/staging/": 7, "production/instances/": 7, "production/runs/": 90, "production/reports/": 90})
        for rule in rules:
            self.assertEqual(rule["NoncurrentVersionExpiration"], {"NoncurrentDays": 1})
            self.assertEqual(rule["AbortIncompleteMultipartUpload"], {"DaysAfterInitiation": 1})
        self.assertNotIn("ArtifactBucket", TEMPLATE["Parameters"])
        table = TEMPLATE["Resources"]["LeaseTable"]
        self.assertNotIn("TimeToLiveSpecification", table["Properties"])
        self.assertEqual(table["DeletionPolicy"], "Retain")
        policy = TEMPLATE["Resources"]["ArtifactBucketPolicy"]["Properties"]["PolicyDocument"]["Statement"]
        self.assertEqual(policy[0]["Effect"], "Deny")
        self.assertEqual(policy[0]["Condition"], {"Bool": {"aws:SecureTransport": "false"}})

    def test_no_implicit_cloud_work_or_broad_runtime_permissions(self):
        types = {value["Type"] for value in TEMPLATE["Resources"].values()}
        self.assertFalse(any(value.startswith(("AWS::EC2::", "AWS::AutoScaling::", "Custom::")) for value in types))
        self.assertNotIn("AWS::Scheduler::Schedule", types)
        self.assertNotIn("AWS::IAM::Policy", types)
        self.assertIn("AWS::IAM::InstanceProfile", types)
        self.assertIn("CollectorFunction", TEMPLATE["Resources"])
        allowed_unscoped = {"ec2:DescribeInstances", "ec2:DescribeVolumes", "ec2:DescribeLaunchTemplateVersions"}
        for name, value in TEMPLATE["Resources"].items():
            if value["Type"] != "AWS::IAM::Role":
                continue
            for statement in statements(name):
                actions = set(sequence(statement["Action"]))
                self.assertFalse(any("*" in action for action in actions), name)
                self.assertFalse(actions & {"dynamodb:TransactWriteItems", "dynamodb:TransactGetItems"})
                if "*" in sequence(statement["Resource"]):
                    self.assertLessEqual(actions, allowed_unscoped, name)
                self.assertFalse(any(action.startswith(("autoscaling:", "ssm:", "kms:")) for action in actions))
                self.assertFalse(actions & {"dynamodb:Scan", "dynamodb:BatchWriteItem", "dynamodb:DeleteTable",
                                           "s3:DeleteObject", "s3:DeleteObjectVersion", "lambda:UpdateFunctionCode"})
            total = sum(len(json.dumps(policy["PolicyDocument"], separators=(",", ":")))
                        for policy in value["Properties"]["Policies"])
            self.assertLess(total, 10_240, name)

    def test_collector_is_disabled_scheduled_retention_with_no_submission_credentials(self):
        resources = TEMPLATE["Resources"]
        function = resources["CollectorFunction"]["Properties"]
        self.assertEqual(function["MemorySize"], 512)
        self.assertEqual(function["Timeout"], 180)
        env = resolve(function["Environment"]["Variables"], False)
        self.assertEqual(env["RCP_PRODUCTION_ENABLED"], "false")
        self.assertEqual(env["RCP_PRODUCTION_MACHINE_ARN"], MACHINE)
        self.assertEqual(env["RCP_PRODUCTION_REPOSITORY_ID"], "655073755")
        self.assertNotIn("RCP_PRODUCTION_AUDIENCE", env)
        rule = resources["CollectorRule"]["Properties"]
        self.assertEqual(rule["ScheduleExpression"], "rate(5 minutes)")
        self.assertEqual(resolve(rule["State"], False), "DISABLED")
        self.assertEqual(resolve(rule["State"], True, False), "ENABLED")
        self.assertEqual(rule["Targets"][0]["Input"], "{}")
        self.assertEqual(rule["Targets"][0]["RetryPolicy"], {"MaximumEventAgeInSeconds": 60, "MaximumRetryAttempts": 0})
        permission = resources["CollectorPermission"]["Properties"]
        self.assertEqual(permission["Principal"], "events.amazonaws.com")
        self.assertEqual(permission["SourceAccount"], {"Ref": "AWS::AccountId"})
        self.assertEqual(permission["SourceArn"], {"Fn::GetAtt": ["CollectorRule", "Arn"]})
        self.assertEqual(resources["CollectorAsyncPolicy"]["Properties"]["MaximumRetryAttempts"], 0)
        for action, resource in (("ec2:RunInstances", INSTANCE), ("ec2:TerminateInstances", INSTANCE),
                                 ("iam:PassRole", VALUES["WorkerRole.Arn"]), ("states:StartExecution", MACHINE),
                                 ("states:StopExecution", MACHINE), ("lambda:InvokeFunction", VALUES["AdmissionFunction.Arn"])):
            self.assertFalse(permits("CollectorRole", action, resource), action)
        for action in ("ec2:DescribeInstances", "ec2:DescribeVolumes"):
            self.assertTrue(permits("CollectorRole", action, "*"))
        schedule = "arn:aws:scheduler:us-east-1:123456789012:schedule/" + PREFIX + "-runtime/runtime-1234-1-1"
        self.assertTrue(permits("CollectorRole", "scheduler:GetSchedule", schedule))
        self.assertFalse(permits("CollectorRole", "scheduler:GetSchedule", schedule.replace("/runtime-", "/other-")))
        for action in ("scheduler:CreateSchedule", "scheduler:UpdateSchedule", "scheduler:DeleteSchedule"):
            self.assertFalse(permits("CollectorRole", action, schedule))

    def test_collector_updates_only_retirement_attributes_and_deletes_only_active(self):
        store = RunStore(None, "production", RUN, "a" * 64)
        update = store._state_update({}, 0, retire="b" * 64)["Update"]
        attributes = sorted(set(update["Key"]) | set(update["ExpressionAttributeNames"].values()))
        context = db_context(RUN_PK) | {"dynamodb:Attributes": attributes}
        self.assertTrue(permits("CollectorRole", "dynamodb:UpdateItem", TABLE, context))
        self.assertFalse(permits("CollectorRole", "dynamodb:UpdateItem", TABLE, db_context(RUN_PK)))
        for forbidden in ("state", "admission", "fingerprint"):
            self.assertFalse(permits("CollectorRole", "dynamodb:UpdateItem", TABLE,
                                    context | {"dynamodb:Attributes": attributes + [forbidden]}), forbidden)
        self.assertFalse(permits("CollectorRole", "dynamodb:UpdateItem", TABLE,
                                db_context(RUN_PK, transaction=False) | {"dynamodb:Attributes": attributes}))
        self.assertTrue(permits("CollectorRole", "dynamodb:DeleteItem", TABLE, db_context("active")))
        self.assertFalse(permits("CollectorRole", "dynamodb:DeleteItem", TABLE, db_context("active", transaction=False)))
        self.assertFalse(permits("CollectorRole", "dynamodb:DeleteItem", TABLE, db_context(RUN_PK)))
        for key in ("policy", "guard", "budget#total", "budget#month#2026-10", "rate#day#2026-10-22", "schedule#slot"):
            for action in ("GetItem", "PutItem", "UpdateItem", "DeleteItem"):
                candidate = db_context(key) | {"dynamodb:Attributes": attributes}
                self.assertFalse(permits("CollectorRole", "dynamodb:" + action, TABLE, candidate), (key, action))
        for key in ("active", RUN_PK):
            self.assertTrue(permits("CollectorRole", "dynamodb:GetItem", TABLE, db_context(key)))
            self.assertFalse(permits("CollectorRole", "dynamodb:PutItem", TABLE, db_context(key)))

    def test_collector_retains_only_fixed_files_and_lists_only_optional_exact_keys(self):
        names = ["durable.json", "status.json", "results.zip", "controller.json", "report.json", "summary.html", "manifest.json"]
        prefix = "production/runs/" + RUN + "/collection/"
        write = {"s3:if-none-match": "*", "s3:x-amz-server-side-encryption": "AES256"}
        for name in names:
            key = prefix + name
            resource = BUCKET_ARN + "/" + key
            self.assertTrue(permits("CollectorRole", "s3:PutObject", resource, write), name)
            self.assertFalse(permits("CollectorRole", "s3:PutObject", resource, {}), name)
            for action in ("s3:GetObject", "s3:GetObjectVersion"):
                self.assertTrue(permits("CollectorRole", action, resource))
            self.assertTrue(permits("CollectorRole", "s3:ListBucket", BUCKET_ARN, {"s3:prefix": key, "s3:max-keys": 1}))
            self.assertFalse(permits("CollectorRole", "s3:ListBucket", BUCKET_ARN, {"s3:prefix": key, "s3:max-keys": 2}))
        for tail in ("output/status.json", "output/results.zip"):
            key = "production/instances/" + INSTANCE + "/" + tail
            for action in ("s3:GetObject", "s3:GetObjectVersion"):
                self.assertTrue(permits("CollectorRole", action, BUCKET_ARN + "/" + key))
            self.assertTrue(permits("CollectorRole", "s3:ListBucket", BUCKET_ARN, {"s3:prefix": key, "s3:max-keys": 1}))
            self.assertFalse(permits("CollectorRole", "s3:PutObject", BUCKET_ARN + "/" + key, write))
        for key in (prefix + "extra.json", "production/runs/" + RUN + "/durable-receipt.json",
                    "production/staging/" + RUN + "/baseline/rcp", "production/runs/repo-999-run-1234/collection/report.json"):
            self.assertFalse(permits("CollectorRole", "s3:PutObject", BUCKET_ARN + "/" + key, write), key)
        for prefix in ("production/", "production/runs/", "production/instances/", prefix,
                       "production/instances/" + OTHER_INSTANCE + "/input/request.json"):
            self.assertFalse(permits("CollectorRole", "s3:ListBucket", BUCKET_ARN, {"s3:prefix": prefix, "s3:max-keys": 1}), prefix)

    def test_intrinsic_references_resolve_without_dependency_cycles(self):
        resources, parameters = TEMPLATE["Resources"], TEMPLATE["Parameters"]
        graph = {name: set() for name in resources}
        def reference(target, owner):
            if target.startswith(("AWS::", "!")):
                return
            target = target.split(".")[0]
            self.assertTrue(target in resources or target in parameters, (owner, target))
            if target in resources:
                graph[owner].add(target)
        def walk(value, owner):
            if isinstance(value, list):
                for item in value:
                    walk(item, owner)
            elif isinstance(value, dict):
                if "Ref" in value:
                    reference(value["Ref"], owner)
                elif "Fn::GetAtt" in value:
                    reference(value["Fn::GetAtt"][0], owner)
                elif "Fn::Sub" in value:
                    for target in re.findall(r"\$\{([^}]+)\}", value["Fn::Sub"]):
                        reference(target, owner)
                elif "Fn::If" in value:
                    self.assertIn(value["Fn::If"][0], TEMPLATE["Conditions"])
                    walk(value["Fn::If"][1:], owner)
                else:
                    for item in value.values():
                        walk(item, owner)
        def visit(name, path):
            self.assertNotIn(name, path, path + [name])
            for dependency in graph[name]:
                visit(dependency, path + [name])
        for name, value in resources.items():
            walk(value, name)
            for dependency in sequence(value.get("DependsOn", [])):
                self.assertIn(dependency, resources)
                graph[name].add(dependency)
        for name in graph:
            visit(name, [])

    def test_guard_dependencies_are_retained_and_code_is_version_pinned(self):
        for name in ("LeaseTable", "ArtifactBucket", "ArtifactBucketPolicy", "WorkerRole", "WorkerInstanceProfile", "RuntimeScheduleGroup",
                     "SchedulerTerminationRole", "WatchdogRole", "WatchdogLogGroup", "WatchdogFunction",
                     "WatchdogRule", "WatchdogPermission", "WatchdogAsyncPolicy", "CollectorRole", "CollectorLogGroup",
                     "CollectorFunction", "CollectorRule", "CollectorPermission", "CollectorAsyncPolicy"):
            value = TEMPLATE["Resources"][name]
            self.assertEqual(value["DeletionPolicy"], "Retain", name)
            self.assertEqual(value["UpdateReplacePolicy"], "Retain", name)
        for name, entrypoint in (("Admission", "production_handler.lambda_handler"), ("Coordinator", "production_runtime.coordinator"),
                                 ("Watchdog", "production_runtime.watchdog"), ("Collector", "production_collection_handler.lambda_handler")):
            props = TEMPLATE["Resources"][name + "Function"]["Properties"]
            self.assertEqual(props["Handler"], "infra.aws_benchmark." + entrypoint)
            self.assertEqual(props["Runtime"], "python3.12")
            self.assertEqual(props["Architectures"], ["x86_64"])
            self.assertEqual(props["Code"]["S3ObjectVersion"], {"Ref": "CodeObjectVersion"})
            self.assertEqual(props["Timeout"], 180 if name == "Collector" else 45)
            self.assertNotIn("VpcConfig", props)
            self.assertEqual(TEMPLATE["Resources"][name + "LogGroup"]["Properties"]["RetentionInDays"], 7)
        self.assertIsNone(re.fullmatch(TEMPLATE["Parameters"]["CodeObjectVersion"]["AllowedPattern"], "null"))


class PublicationRoleTests(unittest.TestCase):
    role = "GitHubPublicationRole"
    collection = BUCKET_ARN + "/production/runs/" + RUN + "/collection/"

    def test_publication_gate_defaults_off_and_is_independent_of_paid_runtime(self):
        parameter = TEMPLATE["Parameters"]["PublicationEnabled"]
        self.assertEqual(parameter["Default"], "false")
        self.assertEqual(parameter["AllowedValues"], ["false", "true"])
        self.assertEqual(TEMPLATE["Conditions"]["Publishing"],
                         {"Fn::Equals": [{"Ref": "PublicationEnabled"}, "true"]})
        props = TEMPLATE["Resources"][self.role]["Properties"]
        for enabled in (False, True):
            for admission in (False, True):
                for publication in (False, True):
                    flags = dict(enabled=enabled, admission=admission, publication=publication)
                    trust = resolve(props["AssumeRolePolicyDocument"], **flags)["Statement"]
                    self.assertEqual(trust[0]["Effect"], "Allow" if publication else "Deny")
                    self.assertTrue(all(entry["Effect"] == ("Allow" if publication else "Deny")
                                        for entry in statements(self.role, **flags)))
                    self.assertEqual(permits(self.role, "dynamodb:GetItem", TABLE,
                        db_context("active", transaction=False), **flags), publication)
                    self.assertEqual(permits(self.role, "s3:GetObject", self.collection + "manifest.json", **flags),
                                     publication)
        self.assertEqual(TEMPLATE["Outputs"]["GitHubPublicationRoleArn"]["Value"],
                         {"Fn::GetAtt": [self.role, "Arn"]})

    def test_publication_trust_uses_only_supported_exact_audience_and_main_subject(self):
        props = TEMPLATE["Resources"][self.role]["Properties"]
        trust = resolve(props["AssumeRolePolicyDocument"])["Statement"]
        context = {"token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                   "token.actions.githubusercontent.com:sub": "repo:wykurz/rcp:ref:refs/heads/main"}
        self.assertEqual(trust, [{"Effect": "Allow", "Action": "sts:AssumeRoleWithWebIdentity",
            "Principal": {"Federated": VALUES["GitHubOidcProviderArn"]}, "Condition": {"StringEquals": context}}])
        self.assertEqual(props["MaxSessionDuration"], 3600)
        self.assertNotIn("ManagedPolicyArns", props)
        self.assertTrue(conditions_match(trust[0]["Condition"], context))
        subjects = ["repo:wykurz/rcp:pull_request", "repo:wykurz/rcp:environment:production",
                    "repo:wykurz/rcp:ref:refs/heads/debug", "repo:wykurz/rcp:ref:refs/tags/main",
                    "repo:another/rcp:ref:refs/heads/main", "repo:wykurz/other:ref:refs/heads/main"]
        for subject in subjects:
            self.assertFalse(conditions_match(trust[0]["Condition"],
                context | {"token.actions.githubusercontent.com:sub": subject}), subject)
        for key in context:
            missing = dict(context)
            del missing[key]
            self.assertFalse(conditions_match(trust[0]["Condition"], missing))
            self.assertFalse(conditions_match(trust[0]["Condition"], context | {key: "different"}))

    def test_publication_reads_only_exact_active_and_repository_run_records(self):
        for key in ("active", RUN_PK, "run#repo-655073755-run-999999"):
            self.assertTrue(permits(self.role, "dynamodb:GetItem", TABLE, db_context(key, transaction=False)))
        for key in ("policy", "guard", "budget#total", "budget#month#2026-10", "rate#day#2026-10-22",
                    "schedule#slot", "run#repo-1-run-1234", "active#other", "run#1234"):
            self.assertFalse(permits(self.role, "dynamodb:GetItem", TABLE, db_context(key, transaction=False)), key)
        self.assertFalse(permits(self.role, "dynamodb:GetItem", TABLE, {}))
        self.assertFalse(permits(self.role, "dynamodb:GetItem", TABLE, db_context(RUN_PK, "policy")))
        self.assertFalse(permits(self.role, "dynamodb:GetItem", TABLE + "-other", db_context(RUN_PK)))
        for action in ("Scan", "Query", "BatchGetItem", "PutItem", "UpdateItem", "DeleteItem", "ConditionCheckItem"):
            for key in ("active", RUN_PK, "policy", "budget#total"):
                self.assertFalse(permits(self.role, "dynamodb:" + action, TABLE, db_context(key)), (action, key))

    def test_publication_inventory_is_exact_repository_prefix_and_reader_page_size(self):
        from infra.aws_benchmark.production_publication import PAGE_SIZE, Reader
        reader = Reader(None, None, bucket=BUCKET, table="production", repository_id="655073755")
        self.assertEqual(PAGE_SIZE, 500)
        context = {"s3:prefix": reader.prefix, "s3:max-keys": PAGE_SIZE}
        self.assertTrue(permits(self.role, "s3:ListBucket", BUCKET_ARN, context))
        for prefix in ("", "production/", "production/runs/", "production/staging/", "production/instances/",
                       "production/runs/repo-1-run-", reader.prefix + "1234/collection/"):
            self.assertFalse(permits(self.role, "s3:ListBucket", BUCKET_ARN, context | {"s3:prefix": prefix}), prefix)
        for max_keys in (1, 499, 501, 1000):
            self.assertFalse(permits(self.role, "s3:ListBucket", BUCKET_ARN, context | {"s3:max-keys": max_keys}))
        for key in context:
            missing = dict(context)
            del missing[key]
            self.assertFalse(permits(self.role, "s3:ListBucket", BUCKET_ARN, missing))
        self.assertFalse(permits(self.role, "s3:ListBucket", BUCKET_ARN + "-other", context))
        self.assertFalse(permits(self.role, "s3:ListBucketVersions", BUCKET_ARN, context))
        self.assertFalse(permits(self.role, "s3:ListAllMyBuckets", "*", context))

    def test_publication_payload_reads_require_versions_and_only_fixed_collection_names(self):
        from infra.aws_benchmark.production_publication import LIMITS
        names = set(LIMITS) | {"manifest.json"}
        self.assertEqual(names, {"manifest.json", "durable.json", "status.json", "results.zip",
                                 "controller.json", "report.json", "summary.html"})
        for name in names:
            self.assertTrue(permits(self.role, "s3:GetObjectVersion", self.collection + name), name)
            self.assertEqual(permits(self.role, "s3:GetObject", self.collection + name), name == "manifest.json", name)
        forbidden = [self.collection + "credentials.json", self.collection + "manifest.json/extra",
            self.collection.replace("repo-655073755-", "repo-1-") + "manifest.json",
            BUCKET_ARN + "/production/runs/" + RUN + "/durable-receipt.json",
            BUCKET_ARN + "/production/staging/" + RUN + "/baseline/rcp",
            instance_key("output/results.zip"), self.collection.replace(BUCKET_ARN, BUCKET_ARN + "-other") + "manifest.json"]
        for resource in forbidden:
            for action in ("s3:GetObject", "s3:GetObjectVersion"):
                self.assertFalse(permits(self.role, action, resource), (action, resource))

    def test_publication_has_no_mutation_execution_role_passing_or_runtime_read_authority(self):
        actions = {action for entry in statements(self.role) for action in sequence(entry["Action"])}
        self.assertEqual(actions, {"dynamodb:GetItem", "s3:ListBucket", "s3:GetObject", "s3:GetObjectVersion"})
        denied = [("s3:PutObject", self.collection + "manifest.json"),
            ("s3:DeleteObject", self.collection + "manifest.json"),
            ("s3:DeleteObjectVersion", self.collection + "manifest.json"),
            ("s3:PutObjectTagging", self.collection + "manifest.json"),
            ("ec2:RunInstances", INSTANCE), ("ec2:TerminateInstances", INSTANCE), ("ec2:DescribeInstances", "*"),
            ("iam:PassRole", VALUES["WorkerRole.Arn"]), ("sts:AssumeRole", VALUES["WorkerRole.Arn"]),
            ("lambda:InvokeFunction", VALUES["AdmissionFunction.Arn"]), ("states:StartExecution", MACHINE),
            ("states:DescribeExecution", MACHINE.replace(":stateMachine:", ":execution:") + ":1234-1"),
            ("scheduler:CreateSchedule", "*"), ("ssm:SendCommand", "*"), ("dynamodb:Scan", TABLE)]
        for action, resource in denied:
            self.assertFalse(permits(self.role, action, resource), (action, resource))


if __name__ == "__main__":
    unittest.main()
