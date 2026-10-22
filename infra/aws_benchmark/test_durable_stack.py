"""Offline permission-boundary checks of the additive durable stack.

This deliberately small evaluator covers the policy operators this template uses;
it is not a replacement for AWS policy simulation and deployed integration tests.
"""

import fnmatch
from pathlib import Path
import re
import unittest

import yaml


ROOT = Path(__file__).resolve().parent
TEMPLATE = yaml.safe_load((ROOT / "durable-stack.yaml").read_text())
ASG = "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:group-id:autoScalingGroupName/rcp"
TERMINATOR = "arn:aws:iam::123456789012:role/termination"
TABLE = "arn:aws:dynamodb:us-east-1:123456789012:table/lease"
MACHINE = "arn:aws:states:us-east-1:123456789012:stateMachine:rcp-capacity"
VALUES = {
    "AWS::Partition": "aws", "AWS::Region": "us-east-1", "AWS::AccountId": "123456789012",
    "AutoScalingGroupArn": ASG, "AutoScalingGroupName": "rcp", "ArtifactBucket": "rcp-artifacts",
    "ResourcePrefix": "rcp", "RuntimeScheduleGroup": "rcp-runtime",
    "LaunchTemplateId": "lt-00000000000000001", "WorkerAmiId": "ami-00000000000000001",
    "WorkerSubnetId": "subnet-00000000000000001", "WorkerSecurityGroupId": "sg-00000000000000001",
    "WorkerInstanceProfileArn": "arn:aws:iam::123456789012:instance-profile/worker",
    "WorkerRoleArn": "arn:aws:iam::123456789012:role/worker",
    "SchedulerTerminationRole.Arn": TERMINATOR, "LeaseTable.Arn": TABLE,
    "CoordinatorFunction.Arn": "arn:aws:lambda:us-east-1:123456789012:function:rcp-coordinator",
    "CoordinatorStateMachine": MACHINE, "CoordinatorStateMachine.Name": "rcp-capacity",
}


def resolve(value, enabled=True, admission=True):
    if isinstance(value, list):
        return [resolve(item, enabled, admission) for item in value]
    if not isinstance(value, dict):
        return value
    if set(value) == {"Ref"}:
        return VALUES[value["Ref"]]
    if set(value) == {"Fn::GetAtt"}:
        return VALUES[".".join(value["Fn::GetAtt"])]
    if set(value) == {"Fn::If"}:
        condition, yes, no = value["Fn::If"]
        assert condition in {"Enabled", "Admitting"}
        active = enabled if condition == "Enabled" else enabled and admission
        return resolve(yes if active else no, enabled, admission)
    if set(value) == {"Fn::Sub"}:
        return re.sub(r"\$\{([^}]+)\}", lambda match: str(VALUES[match[1]]), value["Fn::Sub"])
    return {key: resolve(item, enabled, admission) for key, item in value.items()}


def statements(role, enabled=True, admission=True):
    return [resolve(statement, enabled, admission)
            for policy in TEMPLATE["Resources"][role]["Properties"]["Policies"]
            for statement in policy["PolicyDocument"]["Statement"]]


def sequence(value):
    return value if isinstance(value, list) else [value]


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
            elif operator == "ForAllValues:StringEquals":
                if not all(item in sequence(expected) for item in sequence(context.get(key, []))):
                    return False
            else:
                raise AssertionError("unsupported policy operator: " + operator)
    return True


def permits(role, action, resource, context=None, enabled=True, admission=True):
    allowed = False
    for statement in statements(role, enabled, admission):
        if not any(fnmatch.fnmatchcase(action, item) for item in sequence(statement["Action"])):
            continue
        if not any(fnmatch.fnmatchcase(resource, item) for item in sequence(statement["Resource"])):
            continue
        if not conditions_match(statement.get("Condition", {}), context or {}):
            continue
        if statement["Effect"] == "Deny":
            return False
        allowed |= statement["Effect"] == "Allow"
    return allowed


TAG_KEYS = ["rcp:namespace", "rcp:run", "rcp:attempt", "rcp:deadline", "rcp:created-at"]


def ec2(resource, account="123456789012"):
    return "arn:aws:ec2:us-east-1:" + account + ":" + resource


def launch_context():
    return {"ec2:LaunchTemplate": ec2("launch-template/" + VALUES["LaunchTemplateId"]),
            "ec2:InstanceProfile": VALUES["WorkerInstanceProfileArn"], "ec2:InstanceType": "c5d.24xlarge",
            "ec2:InstanceMarketType": "on-demand", "ec2:Tenancy": "default",
            "ec2:MetadataHttpTokens": "required", "ec2:MetadataHttpEndpoint": "enabled",
            "ec2:MetadataHttpPutResponseHopLimit": 1, "ec2:InstanceMetadataTags": "disabled",
            "ec2:Subnet": ec2("subnet/" + VALUES["WorkerSubnetId"]), "ec2:VolumeType": "gp3",
            "ec2:VolumeSize": 16, "ec2:Encrypted": "true", "aws:TagKeys": TAG_KEYS,
            **{"aws:RequestTag/" + key: value for key, value in zip(TAG_KEYS,
               ["rcp", "123-1", "123-1-1", "2026-10-22T03:00:00Z", "2026-10-22T00:00:00Z"])}}


class DurableStackTests(unittest.TestCase):
    def test_default_stack_cannot_start_background_work(self):
        self.assertEqual(TEMPLATE["Parameters"]["RuntimeEnabled"]["Default"], "false")
        resources = TEMPLATE["Resources"]
        self.assertEqual(resources["WatchdogRule"]["Properties"]["State"],
                         {"Fn::If": ["Enabled", "ENABLED", "DISABLED"]})
        for name in ("CoordinatorFunction", "WatchdogFunction"):
            function = resources[name]["Properties"]
            self.assertEqual(function["ReservedConcurrentExecutions"], {"Fn::If": ["Enabled", 1, 0]})
            if name == "CoordinatorFunction":
                self.assertEqual(function["Environment"]["Variables"]["RCP_DURABLE_ENABLED"],
                                 {"Fn::If": ["Enabled", "true", "false"]})
            else:
                self.assertNotIn("RCP_DURABLE_ENABLED", function["Environment"]["Variables"])
        self.assertFalse(permits("GitHubSubmitRole", "states:StartExecution", MACHINE, enabled=False))
        self.assertTrue(permits("GitHubSubmitRole", "states:StartExecution", MACHINE, enabled=True))

    def test_closing_admission_keeps_existing_execution_and_watchdog_running(self):
        self.assertEqual(TEMPLATE["Parameters"]["AdmissionEnabled"]["Default"], "false")
        resources = TEMPLATE["Resources"]
        watchdog = resources["WatchdogFunction"]["Properties"]
        coordinator = resources["CoordinatorFunction"]["Properties"]
        self.assertEqual(resolve(watchdog["ReservedConcurrentExecutions"], True, False), 1)
        self.assertEqual(resolve(coordinator["ReservedConcurrentExecutions"], True, False), 1)
        self.assertEqual(resolve(coordinator["Environment"]["Variables"]["RCP_DURABLE_ENABLED"], True, False), "true")
        self.assertEqual(resolve(resources["WatchdogRule"]["Properties"]["State"], True, False), "ENABLED")
        self.assertFalse(permits("GitHubSubmitRole", "states:StartExecution", MACHINE,
                                enabled=True, admission=False))

    def test_launch_requires_pinned_sources_and_complete_creation_tags(self):
        context = launch_context()
        resources = [ec2("image/" + VALUES["WorkerAmiId"], account=""),
                     ec2("launch-template/" + VALUES["LaunchTemplateId"]),
                     ec2("subnet/" + VALUES["WorkerSubnetId"]),
                     ec2("security-group/" + VALUES["WorkerSecurityGroupId"]),
                     ec2("instance/i-new"), ec2("network-interface/eni-new"), ec2("volume/vol-new")]
        for target in resources:
            self.assertTrue(permits("CoordinatorRole", "ec2:RunInstances", target, context), target)
            wrong_template = dict(context, **{"ec2:LaunchTemplate": ec2("launch-template/lt-other")})
            self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances", target, wrong_template))
        for kind in ("image/ami-other", "launch-template/lt-other", "subnet/subnet-other",
                     "security-group/sg-other", "key-pair/key", "spot-instances-request/sir-other"):
            self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances", ec2(kind), context))
        for key in TAG_KEYS:
            missing = {k: v for k, v in context.items() if k != "aws:RequestTag/" + key}
            for kind in ("instance/i-new", "volume/vol-new"):
                self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances", ec2(kind), missing), key)
        extra = dict(context, **{"aws:TagKeys": TAG_KEYS + ["administrator"]})
        self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances", ec2("instance/i-new"), extra))

    def test_launch_rejects_wrong_instance_network_storage_or_identity(self):
        cases = [("instance/i-new", "ec2:InstanceType", "c5d.metal"),
                 ("instance/i-new", "ec2:InstanceMarketType", "spot"),
                 ("instance/i-new", "ec2:InstanceProfile", VALUES["WorkerInstanceProfileArn"] + "-other"),
                 ("instance/i-new", "ec2:MetadataHttpTokens", "optional"),
                 ("instance/i-new", "ec2:MetadataHttpPutResponseHopLimit", 2),
                 ("instance/i-new", "aws:RequestTag/rcp:namespace", "other"),
                 ("network-interface/eni-new", "ec2:Subnet", ec2("subnet/subnet-other")),
                 ("volume/vol-new", "ec2:VolumeType", "io2"),
                 ("volume/vol-new", "ec2:VolumeSize", 17),
                 ("volume/vol-new", "ec2:Encrypted", "false")]
        for target, key, value in cases:
            context = dict(launch_context(), **{key: value})
            self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances", ec2(target), context), key)
        self.assertFalse(permits("CoordinatorRole", "ec2:RunInstances",
                                "arn:aws:ec2:us-west-2:123456789012:instance/i-new", launch_context()))

    def test_only_creation_can_apply_ownership_tags(self):
        context = dict(launch_context(), **{"ec2:CreateAction": "RunInstances"})
        for kind in ("instance/i-new", "volume/vol-new"):
            self.assertTrue(permits("CoordinatorRole", "ec2:CreateTags", ec2(kind), context))
            self.assertFalse(permits("CoordinatorRole", "ec2:CreateTags", ec2(kind), launch_context()))
            self.assertFalse(permits("CoordinatorRole", "ec2:CreateTags", ec2(kind),
                                    dict(context, **{"ec2:CreateAction": "CreateVolume"})))
        for role in ("CoordinatorRole", "WatchdogRole", "SchedulerTerminationRole", "GitHubSubmitRole"):
            for action in ("ec2:DeleteTags", "ec2:ModifyInstanceAttribute", "ec2:ModifyInstanceMetadataOptions",
                           "ec2:CreateLaunchTemplateVersion", "ec2:CreateVolume", "ec2:StartInstances"):
                self.assertFalse(permits(role, action, ec2("instance/i-new"), context), (role, action))
            self.assertFalse(permits(role, "autoscaling:UpdateAutoScalingGroup", ASG))
            self.assertFalse(permits(role, "autoscaling:SetDesiredCapacity", ASG))

    def test_termination_is_managed_namespace_scoped_and_cannot_launch(self):
        for role in ("WatchdogRole", "SchedulerTerminationRole"):
            self.assertTrue(permits(role, "ec2:TerminateInstances", ec2("instance/i-managed"),
                                   {"ec2:ResourceTag/rcp:namespace": "rcp"}))
            for context in ({}, {"ec2:ResourceTag/rcp:namespace": "other"}):
                self.assertFalse(permits(role, "ec2:TerminateInstances", ec2("instance/i-managed"), context))
            self.assertFalse(permits(role, "ec2:RunInstances", ec2("instance/i-managed"), launch_context()))
        self.assertFalse(permits("CoordinatorRole", "ec2:TerminateInstances", ec2("instance/i-managed"),
                                {"ec2:ResourceTag/rcp:namespace": "rcp"}))

    def test_runtime_roles_cannot_change_existing_cutoffs(self):
        runtime = "arn:aws:scheduler:us-east-1:123456789012:schedule/rcp-runtime/runtime-123-1"
        other = "arn:aws:scheduler:us-east-1:123456789012:schedule/other/runtime-123-1"
        self.assertTrue(permits("WatchdogRole", "scheduler:CreateSchedule", runtime))
        self.assertFalse(permits("WatchdogRole", "scheduler:CreateSchedule", other))
        for role in ("CoordinatorRole", "WatchdogRole", "GitHubSubmitRole", "SchedulerTerminationRole"):
            for target in (runtime, other, "arn:aws:scheduler:us-east-1:123456789012:schedule-group/rcp-runtime"):
                for action in ("scheduler:UpdateSchedule", "scheduler:DeleteSchedule",
                               "scheduler:DeleteScheduleGroup", "scheduler:CreateScheduleGroup"):
                    self.assertFalse(permits(role, action, target), (role, action, target))
        self.assertFalse(permits("CoordinatorRole", "scheduler:CreateSchedule", runtime))

    def test_passrole_is_exact_for_each_service(self):
        for role, target, service in (("WatchdogRole", TERMINATOR, "scheduler.amazonaws.com"),
                                     ("CoordinatorRole", VALUES["WorkerRoleArn"], "ec2.amazonaws.com")):
            self.assertTrue(permits(role, "iam:PassRole", target, {"iam:PassedToService": service}))
            for wrong in (None, "lambda.amazonaws.com", "ssm.amazonaws.com"):
                context = {} if wrong is None else {"iam:PassedToService": wrong}
                self.assertFalse(permits(role, "iam:PassRole", target, context))
            self.assertFalse(permits(role, "iam:PassRole", target + "-other", {"iam:PassedToService": service}))
        self.assertFalse(permits("CoordinatorRole", "iam:PassRole", VALUES["WorkerInstanceProfileArn"],
                                {"iam:PassedToService": "ec2.amazonaws.com"}))
        self.assertFalse(permits("WatchdogRole", "iam:PassRole", VALUES["WorkerRoleArn"],
                                {"iam:PassedToService": "ec2.amazonaws.com"}))
        for role in ("GitHubSubmitRole", "SchedulerTerminationRole"):
            self.assertFalse(permits(role, "iam:PassRole", TERMINATOR,
                                    {"iam:PassedToService": "scheduler.amazonaws.com"}))

    def test_scheduler_trust_is_scoped_to_this_account_and_runtime_group(self):
        trust = TEMPLATE["Resources"]["SchedulerTerminationRole"]["Properties"]["AssumeRolePolicyDocument"]["Statement"]
        self.assertEqual(len(trust), 1)
        self.assertEqual(trust[0]["Principal"], {"Service": "scheduler.amazonaws.com"})
        self.assertEqual(trust[0]["Condition"], {
            "StringEquals": {"aws:SourceAccount": {"Ref": "AWS::AccountId"}},
            "ArnEquals": {"aws:SourceArn": [{"Fn::GetAtt": ["RuntimeScheduleGroup", "Arn"]}]}})

    def test_lease_and_independent_guard_freshness_have_separate_writers(self):
        for role in ("CoordinatorRole", "WatchdogRole"):
            self.assertTrue(permits(role, "dynamodb:UpdateItem", TABLE, {"dynamodb:LeadingKeys": ["lease"]}))
            for keys in ([], ["other"], ["lease", "other"], ["guard"]):
                context = {"dynamodb:LeadingKeys": keys} if keys else {}
                self.assertFalse(permits(role, "dynamodb:UpdateItem", TABLE, context))
            for action in ("dynamodb:DeleteItem", "dynamodb:Scan", "dynamodb:UpdateTimeToLive"):
                self.assertFalse(permits(role, action, TABLE, {"dynamodb:LeadingKeys": ["lease"]}))
        self.assertTrue(permits("CoordinatorRole", "dynamodb:GetItem", TABLE, {"dynamodb:LeadingKeys": ["guard"]}))
        self.assertFalse(permits("CoordinatorRole", "dynamodb:PutItem", TABLE, {"dynamodb:LeadingKeys": ["guard"]}))
        self.assertTrue(permits("WatchdogRole", "dynamodb:PutItem", TABLE, {"dynamodb:LeadingKeys": ["guard"]}))
        self.assertFalse(permits("WatchdogRole", "dynamodb:PutItem", TABLE, {"dynamodb:LeadingKeys": ["lease"]}))
        table = TEMPLATE["Resources"]["LeaseTable"]
        self.assertNotIn("TimeToLiveSpecification", table["Properties"])
        self.assertEqual(table["Properties"]["BillingMode"], "PAY_PER_REQUEST")
        self.assertEqual(table["DeletionPolicy"], "Retain")

    def test_github_cannot_self_certify_request_or_result(self):
        prefix = "arn:aws:s3:::rcp-artifacts/"
        for role in ("baseline", "candidate"):
            for binary in ("rcp", "rcpd", "filegen"):
                self.assertTrue(permits("GitHubSubmitRole", "s3:PutObject", prefix + "input/" + role + "/" + binary))
        for key in ("control/window.json", "control/durable-request.json", "input/request.json",
                    "output/status.json", "output/results.zip", "output/durable-receipt.json", "input/candidate/shell"):
            self.assertFalse(permits("GitHubSubmitRole", "s3:PutObject", prefix + key), key)
        for action in ("ec2:RunInstances", "ec2:TerminateInstances", "states:StopExecution"):
            self.assertFalse(permits("GitHubSubmitRole", action, ASG))
        self.assertFalse(permits("GitHubSubmitRole", "states:StartExecution", MACHINE + "-other"))

    def test_watchdog_cannot_forge_worker_status_or_results(self):
        for key in ("output/status.json", "output/results.zip"):
            self.assertFalse(permits("WatchdogRole", "s3:PutObject", "arn:aws:s3:::rcp-artifacts/" + key))
        self.assertTrue(permits("WatchdogRole", "s3:GetObject", "arn:aws:s3:::rcp-artifacts/output/status.json"))
        for key in ("control/window.json", "input/request.json", "output/durable-receipt.json"):
            self.assertTrue(permits("WatchdogRole", "s3:PutObject", "arn:aws:s3:::rcp-artifacts/" + key))

    def test_github_trust_has_exact_protected_environment(self):
        trust = TEMPLATE["Resources"]["GitHubSubmitRole"]["Properties"]["AssumeRolePolicyDocument"]["Statement"]
        self.assertEqual(len(trust), 1)
        self.assertEqual(trust[0]["Condition"]["StringEquals"], {
            "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
            "token.actions.githubusercontent.com:sub": "repo:wykurz/rcp:environment:aws-benchmark-durable"})
        self.assertEqual(TEMPLATE["Resources"]["GitHubSubmitRole"]["Properties"]["MaxSessionDuration"], 3600)

    def test_only_service_reads_have_unscoped_resources(self):
        expected = {"ec2:DescribeInstances", "ec2:DescribeVolumes", "ec2:DescribeLaunchTemplateVersions"}
        for role, resource in TEMPLATE["Resources"].items():
            if resource["Type"] != "AWS::IAM::Role":
                continue
            for statement in statements(role):
                if "*" in sequence(statement["Resource"]):
                    self.assertLessEqual(set(sequence(statement["Action"])), expected)
                self.assertFalse(any(action in {"*", "iam:*", "s3:*", "ec2:*", "autoscaling:*"}
                                     for action in sequence(statement["Action"])))

    def test_runtime_read_scope_matches_current_adapter(self):
        self.assertTrue(permits("CoordinatorRole", "ec2:DescribeLaunchTemplateVersions", "*"))
        self.assertFalse(permits("CoordinatorRole", "ec2:DescribeVolumes", "*"))
        self.assertTrue(permits("WatchdogRole", "ec2:DescribeVolumes", "*"))
        self.assertFalse(permits("WatchdogRole", "ec2:DescribeLaunchTemplateVersions", "*"))
        for role in ("CoordinatorRole", "WatchdogRole"):
            for action in ("autoscaling:DescribeAutoScalingGroups", "ec2:DescribeImages",
                           "ec2:DescribeSnapshots", "ssm:GetServiceSetting"):
                self.assertFalse(permits(role, action, "*"), (role, action))
        self.assertFalse(permits("CoordinatorRole", "scheduler:GetSchedule",
                         "arn:aws:scheduler:us-east-1:123456789012:schedule/rcp-runtime/runtime-123-1"))

    def test_independent_shutdown_dependencies_survive_stack_deletion(self):
        for name in ("RuntimeScheduleGroup", "SchedulerTerminationRole", "WatchdogRole", "WatchdogLogGroup",
                     "WatchdogFunction", "WatchdogRule", "WatchdogPermission", "WatchdogAsyncPolicy"):
            resource = TEMPLATE["Resources"][name]
            self.assertEqual(resource["DeletionPolicy"], "Retain")
            self.assertEqual(resource["UpdateReplacePolicy"], "Retain")
        self.assertEqual(set(action for statement in statements("SchedulerTerminationRole")
                             for action in sequence(statement["Action"])), {"ec2:TerminateInstances"})
        self.assertNotIn("OuterScheduleGroup", TEMPLATE["Resources"])

    def test_runtime_roles_cannot_disable_or_reconfigure_independent_guard(self):
        for role in ("CoordinatorRole", "WatchdogRole", "GitHubSubmitRole", "SchedulerTerminationRole"):
            for action in ("lambda:UpdateFunctionConfiguration", "lambda:UpdateFunctionCode",
                           "lambda:DeleteFunction", "lambda:PutFunctionConcurrency", "lambda:RemovePermission",
                           "events:DisableRule", "events:DeleteRule", "events:RemoveTargets",
                           "iam:PutRolePolicy", "iam:DeleteRole", "iam:UpdateAssumeRolePolicy"):
                resource = {"lambda": "arn:aws:lambda:us-east-1:123456789012:function:rcp-watchdog",
                            "events": "arn:aws:events:us-east-1:123456789012:rule/rcp-watchdog",
                            "iam": "arn:aws:iam::123456789012:role/rcp-watchdog"}[action.split(":")[0]]
                self.assertFalse(permits(role, action, resource), (role, action))
        self.assertNotIn("OuterScheduleGroup", TEMPLATE["Resources"])

    def test_function_configuration_matches_direct_adapter_contract(self):
        names = {"ENABLED", "REGION", "ACCOUNT", "NAMESPACE", "TABLE", "BUCKET", "LT_ID", "LT_VERSION",
                 "AMI", "SOURCE", "SUBNET", "SECURITY_GROUP", "PROFILE_ARN", "RUNTIME_GROUP", "TERMINATION_ROLE"}
        for name in ("CoordinatorFunction", "WatchdogFunction"):
            env = TEMPLATE["Resources"][name]["Properties"]["Environment"]["Variables"]
            self.assertEqual(set(env), {"RCP_DURABLE_" + key for key in
                                       (names if name == "CoordinatorFunction" else names - {"ENABLED"})})
        for role in ("CoordinatorRole", "WatchdogRole"):
            self.assertFalse(permits(role, "kms:CreateGrant", "*"))
            self.assertFalse(permits(role, "kms:Decrypt", "*"))

    def test_no_instance_network_or_existing_worker_iam_resource_is_created(self):
        types = {resource["Type"] for resource in TEMPLATE["Resources"].values()}
        self.assertFalse(any(kind.startswith(("AWS::EC2::", "AWS::AutoScaling::")) for kind in types))
        self.assertNotIn("AWS::IAM::Policy", types)
        self.assertNotIn("AWS::Scheduler::Schedule", types)
        self.assertNotIn("AWS::IAM::InstanceProfile", types)

    def test_function_code_is_version_pinned_and_logs_are_bounded(self):
        for name in ("CoordinatorFunction", "WatchdogFunction"):
            props = TEMPLATE["Resources"][name]["Properties"]
            self.assertEqual(props["Code"]["S3ObjectVersion"], {"Ref": "CodeObjectVersion"})
            self.assertEqual(props["Timeout"], 45)
            self.assertNotIn("VpcConfig", props)
        pattern = TEMPLATE["Parameters"]["CodeObjectVersion"]["AllowedPattern"]
        self.assertIsNone(re.fullmatch(pattern, "null"))
        self.assertIsNone(re.fullmatch(pattern, ""))
        for name in ("CoordinatorLogGroup", "WatchdogLogGroup"):
            self.assertEqual(TEMPLATE["Resources"][name]["Properties"]["RetentionInDays"], 7)

    def test_state_machine_retries_service_failures_with_bound_execution_identity(self):
        machine = TEMPLATE["Resources"]["CoordinatorStateMachine"]["Properties"]
        self.assertEqual(machine["StateMachineType"], "STANDARD")
        definition = machine["Definition"]
        self.assertGreaterEqual(definition["TimeoutSeconds"], 16 * 3600)
        observe = definition["States"]["Observe"]
        self.assertEqual(observe["Parameters"], {
            "run.$": "$.run", "execution_arn.$": "$$.Execution.Id", "execution_name.$": "$$.Execution.Name"})
        retry = observe["Retry"]
        self.assertEqual(len(retry), 1)
        self.assertEqual(set(retry[0]["ErrorEquals"]), {
            "Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException",
            "Lambda.ClientExecutionTimeoutException", "Lambda.TooManyRequestsException",
            "Sandbox.Timedout", "Lambda.Unknown", "States.Timeout"})
        self.assertEqual(retry[0]["MaxAttempts"], 6)
        self.assertEqual(retry[0]["MaxDelaySeconds"], 30)
        self.assertEqual(retry[0]["JitterStrategy"], "FULL")
        self.assertNotIn("States.ALL", retry[0]["ErrorEquals"])
        self.assertEqual(definition["States"]["Pause"], {"Type": "Wait", "Seconds": 60, "Next": "Observe"})
        self.assertEqual(definition["States"]["Decision"]["Default"], "Failed")
        self.assertEqual(observe["Catch"][0]["Next"], "Failed")
        for name in definition["States"]:
            self.assertNotIn("credentials", str(definition["States"][name]).lower())

    def test_polling_intervals_fit_documented_nominal_startup_margins(self):
        from infra.aws_benchmark import durable, worker
        resources = TEMPLATE["Resources"]
        invocation = max(resources[name]["Properties"]["Timeout"]
                         for name in ("CoordinatorFunction", "WatchdogFunction"))
        tick = resources["CoordinatorStateMachine"]["Properties"]["Definition"]["States"]["Pause"]["Seconds"]
        self.assertEqual(resources["WatchdogRule"]["Properties"]["ScheduleExpression"], "rate(1 minute)")
        self.assertLess(invocation, durable.LAUNCH_ACK_SECONDS)
        self.assertLessEqual(durable.LAUNCH_ACK_SECONDS, worker.ASSIGNMENT_WAIT_SECONDS)
        self.assertGreaterEqual(durable.GUARD_GAP_SECONDS, tick + invocation + 15)
        self.assertGreaterEqual(worker.ASSIGNMENT_WAIT_SECONDS, tick + invocation + 15)

    def test_watchdog_delivery_has_no_day_long_stale_queue(self):
        rule = TEMPLATE["Resources"]["WatchdogRule"]["Properties"]
        self.assertEqual(rule["ScheduleExpression"], "rate(1 minute)")
        self.assertEqual(rule["Targets"][0]["RetryPolicy"], {
            "MaximumEventAgeInSeconds": 60, "MaximumRetryAttempts": 0})
        policy = TEMPLATE["Resources"]["WatchdogAsyncPolicy"]["Properties"]
        self.assertEqual(policy["MaximumEventAgeInSeconds"], 60)
        self.assertEqual(policy["MaximumRetryAttempts"], 0)


if __name__ == "__main__":
    unittest.main()
