"""Offline qualified-invocation, durable replay and no-launch wrapper tests."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

from botocore.exceptions import ClientError

from . import admission_probe as probe
from . import admission_acceptance as cases
from .test_admission_acceptance import ShapedTransactions, NOW, TABLE


def bindings():
    nonce = TABLE.rsplit("-", 1)[1]
    name = "rcp-admission-acceptance-" + nonce
    return dict(schema=probe.SCHEMA, account=cases.ACCOUNT, region="us-east-1", nonce=nonce,
        function_name=name, function_version="1", role_arn="arn:aws:iam::" + cases.ACCOUNT + ":role/" + name,
        role_id="AROA" + "A" * 17, log_group="/aws/lambda/" + name, table=TABLE,
        table_id="12345678-1234-1234-1234-123456789abc", source_commit="f" * 40,
        approval_capture_sha256="a" * 64, not_before=int(NOW.timestamp()) - 60,
        expires_at=int(NOW.timestamp()) + 1800, fixture_at=cases.timestamp(NOW))


def context(config):
    return SimpleNamespace(invoked_function_arn="arn:aws:lambda:us-east-1:" + cases.ACCOUNT +
        ":function:" + config["function_name"] + ":1", function_version="1",
        log_group_name=config["log_group"], aws_request_id=str(uuid.uuid4()),
        get_remaining_time_in_millis=lambda: 119000)


def identity(config):
    name = config["role_arn"].rsplit("/", 1)[1]
    return dict(Account=config["account"],
        Arn="arn:aws:sts::" + config["account"] + ":assumed-role/" + name + "/probe",
        UserId=config["role_id"] + ":probe")


class FixtureDynamo(ShapedTransactions):
    def __init__(self, phase, config):
        super().__init__(phase)
        self.config, self.controls, self.closed = config, [], False
        self.claim_response_lost = False
        self.descriptions = 0
        self.replace_on_final_description = False

    def describe_table(self, **request):
        self.controls.append(("describe_table", copy.deepcopy(request)))
        from botocore.validate import validate_parameters
        validate_parameters(request, self.model.operation_model("DescribeTable").input_shape)
        self.descriptions += 1
        value = dict(TableId=self.config["table_id"], TableName=TABLE, TableStatus="ACTIVE",
            TableArn="arn:aws:dynamodb:us-east-1:" + cases.ACCOUNT + ":table/" + TABLE,
            KeySchema=[dict(AttributeName="pk", KeyType="HASH")],
            AttributeDefinitions=[dict(AttributeName="pk", AttributeType="S")],
            BillingModeSummary=dict(BillingMode="PAY_PER_REQUEST"), DeletionProtectionEnabled=True)
        if self.replace_on_final_description and self.descriptions > 1:
            value["TableId"] = "87654321-1234-1234-1234-123456789abc"
        return dict(Table=value)

    def put_item(self, **request):
        self.controls.append(("put_item", copy.deepcopy(request)))
        from botocore.validate import validate_parameters
        validate_parameters(request, self.model.operation_model("PutItem").input_shape)
        key = request["Item"]["pk"]["S"]
        assert key.startswith("probe#") and request["ConditionExpression"] == "attribute_not_exists(pk)"
        with self.lock:
            if key in self.items:
                raise ClientError({"Error": {"Code": "ConditionalCheckFailedException",
                    "Message": "never-emit-service-body"}}, "PutItem")
            self.items[key] = copy.deepcopy(request["Item"])
        if self.claim_response_lost:
            raise TimeoutError("never-emit-uncertain-service-text")
        return {}

    def close(self):
        self.closed = True


class BindingTests(unittest.TestCase):
    def test_canonical_roundtrip_detaches_all_nonsecret_inputs(self):
        cfg = bindings()
        self.assertEqual(probe.parse_bindings(probe.binding_bytes(cfg)), cfg)
        checked = probe.validate_bindings(cfg)
        cfg["source_commit"] = "b" * 40
        self.assertNotEqual(checked, cfg)
        self.assertEqual(len(probe.digest_bindings(checked)), 64)

    def test_invalid_targets_windows_and_extra_fields_fail_before_clients(self):
        mutations = dict(account="123456789012", region="us-west-2", nonce="../",
            function_name="production-admission", function_version="$LATEST", role_arn="root",
            role_id="mutable-name", log_group="/aws/lambda/production", table="production-table",
            table_id="not-a-uuid", source_commit="main", approval_capture_sha256="",
            not_before=True, expires_at=int(NOW.timestamp()) + 10000,
            fixture_at="1970-01-01T00:00:00+00:00", secret="never-emit")
        for key, value in mutations.items():
            factory = Mock()
            cfg = bindings() | {key: value}
            result = probe.run_probe(cfg, {"phase": "dedup"}, context(bindings()),
                factory=factory, clock=lambda: NOW.timestamp(), monotonic=lambda: 0)
            with self.subTest(key=key):
                self.assertFalse(result["ok"])
                self.assertNotIn("never-emit", json.dumps(result))
                factory.assert_not_called()

    def test_duplicate_oversize_or_noncanonical_json_does_not_parse(self):
        for raw in (b'{"schema":"a","schema":"b"}', b"x" * (probe.MAX_BINDINGS + 1), b"[]", b""):
            with self.subTest(raw=raw[:20]), self.assertRaises((ValueError, TypeError)):
                probe.parse_bindings(raw)


class RuntimeTests(unittest.TestCase):
    def run_case(self, phase="dedup", cfg=None, ctx=None, model=None, sts_identity=None, clock=None):
        cfg = cfg or bindings()
        model = model or FixtureDynamo(phase, cfg)
        sts = Mock()
        sts.get_caller_identity.return_value = sts_identity or identity(cfg)
        factory = Mock(side_effect=lambda service, region: {"sts": sts, "dynamodb": model}[service])
        result = probe.run_probe(cfg, {"phase": phase}, ctx or context(cfg), factory=factory,
            clock=clock or (lambda: NOW.timestamp()), monotonic=lambda: 10)
        return result, model, sts, factory

    def test_every_phase_uses_real_cases_but_never_claims_live_acceptance(self):
        for phase in cases.PHASES:
            with self.subTest(phase=phase):
                result, model, sts, factory = self.run_case(phase)
                self.assertTrue(result["ok"], result)
                self.assertFalse(result["live_accepted"])
                self.assertEqual(result["source_commit"], bindings()["source_commit"])
                self.assertEqual(result["observation"]["source_commit"], bindings()["source_commit"])
                self.assertTrue(result["observation"]["no_seal_or_start"])
                self.assertLessEqual(result["calls"], probe.CONTROL_CALL_LIMIT)
                self.assertEqual(factory.call_args_list, [unittest.mock.call("sts", "us-east-1"),
                    unittest.mock.call("dynamodb", "us-east-1")])
                self.assertEqual(len([key for key in model.items if key.startswith("probe#")]), 1)
                self.assertTrue(model.closed)
                for key, item in model.items.items():
                    if key.startswith("run#"):
                        admission = json.loads(item["admission"]["S"])
                        initial = admission["initial_payload"]
                        self.assertEqual(initial["manifest"]["baseline"], bindings()["source_commit"])
                        self.assertEqual(initial["manifest"]["candidate"], bindings()["source_commit"])
                        self.assertEqual(initial["state"]["request"]["pins"]["source"], bindings()["source_commit"])
                sts.close.assert_called_once()

    def test_warm_or_cold_duplicate_claim_is_never_adopted_or_replayed(self):
        first, model, _, _ = self.run_case()
        self.assertTrue(first["ok"])
        writes = len(model.writes)
        second, model, _, _ = self.run_case(model=model)
        self.assertFalse(second["ok"])
        self.assertEqual(second["stage"], "claim")
        self.assertEqual(len(model.writes), writes)
        self.assertNotIn("never-emit", json.dumps(second))

    def test_lost_claim_response_holds_without_case_writes_or_retry(self):
        model = FixtureDynamo("dedup", bindings())
        model.claim_response_lost = True
        result, model, _, _ = self.run_case(model=model)
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "claim")
        self.assertEqual(len(model.writes), 0)
        self.assertEqual(sum(name == "put_item" for name, _ in model.controls), 1)
        self.assertEqual(len([key for key in model.items if key.startswith("probe#")]), 1)
        self.assertNotIn("never-emit", json.dumps(result))

    def test_invalid_events_and_contexts_fail_before_all_clients(self):
        cfg = bindings()
        for event in ({}, {"phase": "unknown"}, {"phase": "dedup", "table": "foreign"},
                      {"phase": ["dedup"]}, {"phase": True}):
            factory = Mock()
            result = probe.run_probe(cfg, event, context(cfg), factory=factory,
                clock=lambda: NOW.timestamp(), monotonic=lambda: 0)
            self.assertFalse(result["ok"])
            factory.assert_not_called()
        for key, value in (("invoked_function_arn", "wrong"), ("function_version", "$LATEST"),
                           ("log_group_name", "wrong"), ("aws_request_id", "wrong")):
            factory = Mock()
            ctx = context(cfg)
            setattr(ctx, key, value)
            self.assertFalse(probe.run_probe(cfg, {"phase": "dedup"}, ctx, factory=factory,
                clock=lambda: NOW.timestamp(), monotonic=lambda: 0)["ok"])
            factory.assert_not_called()

    def test_wrong_account_or_role_id_precedes_fixture_client(self):
        cfg = bindings()
        for key, value in (("Account", "123456789012"), ("UserId", "AROA" + "B" * 17 + ":probe"),
                           ("Arn", cfg["role_arn"])):
            result, model, _, factory = self.run_case(sts_identity=identity(cfg) | {key: value})
            self.assertFalse(result["ok"])
            self.assertEqual(factory.call_count, 1)
            self.assertEqual(model.controls, [])

    def test_wrong_table_or_unprotected_lifecycle_precedes_claim(self):
        cfg = bindings()
        changes = dict(TableId="87654321-1234-1234-1234-123456789abc",
            TableArn="arn:aws:dynamodb:us-east-1:" + cases.ACCOUNT + ":table/production",
            TableStatus="DELETING", DeletionProtectionEnabled=False,
            BillingModeSummary=dict(BillingMode="PROVISIONED"))
        for key, value in changes.items():
            model = FixtureDynamo("dedup", cfg)
            original = model.describe_table
            model.describe_table = lambda **request: dict(Table=original(**request)["Table"] | {key: value})
            result, _, _, _ = self.run_case(model=model)
            self.assertFalse(result["ok"])
            self.assertEqual(result["stage"], "table")
            self.assertFalse(any(name == "put_item" for name, _ in model.controls))

    def test_postcase_table_replacement_retains_committed_fixture_evidence(self):
        model = FixtureDynamo("dedup", bindings())
        model.replace_on_final_description = True
        result, model, _, _ = self.run_case(model=model)
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "table-readback")
        self.assertIn("active", model.items)
        self.assertEqual(len([key for key in model.items if key.startswith("probe#")]), 1)

    def test_wrong_seed_is_a_spent_claim_hold_never_reset(self):
        model = FixtureDynamo("dedup", bindings())
        model.items["active"] = {"pk": {"S": "active"}, "run_key": {"S": "foreign"}}
        result, model, _, _ = self.run_case(model=model)
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "cases")
        self.assertEqual(model.writes, [])
        self.assertEqual(model.items["active"]["run_key"]["S"], "foreign")

    def test_expiry_and_outer_deadline_hold_without_retries(self):
        cfg = bindings()
        for now in (cfg["not_before"] - 1, cfg["expires_at"] - 125, float("nan")):
            factory = Mock()
            self.assertFalse(probe.run_probe(cfg, {"phase": "dedup"}, context(cfg), factory=factory,
                clock=lambda: now, monotonic=lambda: 0)["ok"])
            factory.assert_not_called()
        ctx = context(cfg)
        ctx.get_remaining_time_in_millis = lambda: 5000
        result, model, _, factory = self.run_case(ctx=ctx)
        self.assertFalse(result["ok"])
        factory.assert_not_called()

    def test_mid_case_deadline_hold_preserves_claim_and_never_cleans_up(self):
        cfg = bindings()
        model = FixtureDynamo("dedup", cfg)
        original = model.put_item
        expired = False
        def claim_then_expire(**request):
            nonlocal expired
            result = original(**request)
            expired = True
            return result
        model.put_item = claim_then_expire
        result, model, _, _ = self.run_case(model=model,
            clock=lambda: cfg["expires_at"] if expired else NOW.timestamp())
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "claim")
        self.assertEqual(len([key for key in model.items if key.startswith("probe#")]), 1)
        self.assertEqual(model.writes, [])

    def test_bindings_file_failure_and_duplicate_fields_have_no_client_side_effect(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "bindings.json"
            with patch.object(probe, "BINDING_FILE", file), patch.object(probe, "run_probe") as execute:
                self.assertFalse(probe.lambda_handler({}, None)["ok"])
                file.write_bytes(b'{"schema":1,"schema":2}')
                self.assertFalse(probe.lambda_handler({}, None)["ok"])
                execute.assert_not_called()

    def test_no_secret_launch_or_configured_service_endpoint(self):
        with patch("boto3.client") as create:
            probe.sdk_client("dynamodb", "us-east-1")
        kwargs = create.call_args.kwargs
        self.assertEqual(kwargs["endpoint_url"], "https://dynamodb.us-east-1.amazonaws.com")
        self.assertEqual(kwargs["config"].retries, {"total_max_attempts": 1})
        self.assertEqual(kwargs["config"].proxies, {})
        self.assertTrue(kwargs["config"].ignore_configured_endpoint_urls)
        for service in ("ec2", "s3", "states", "secretsmanager", "iam", "lambda", "scheduler"):
            with self.assertRaises(ValueError):
                probe.sdk_client(service, "us-east-1")


if __name__ == "__main__":
    unittest.main()
