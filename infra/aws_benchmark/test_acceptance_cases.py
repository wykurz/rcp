"""Offline acceptance-core contracts: fake services and local RSA, never live permissions proof."""
import copy
from dataclasses import replace
import io
import json
import unittest
import traceback
from botocore.handlers import handle_copy_source_param
from botocore.session import Session
from botocore.validate import validate_parameters
from unittest.mock import Mock
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from infra.aws_benchmark import acceptance_cases as probes
from infra.aws_benchmark import production_auth as auth
from infra.aws_benchmark.test_production_store import DynamoTransactions

SECRET = "arn:aws:secretsmanager:us-east-1:381183037896:secret:/rcp/production/github-read-app-AbCd12"
OTHER = "arn:aws:secretsmanager:us-east-1:381183037896:secret:/rcp/acceptance/reader-denied-XyZa34"
CURRENT = "1" * 32
PREVIOUS = "2" * 32
READER = probes.Bindings(account="381183037896", region="us-east-1",
    role_arn="arn:aws:iam::381183037896:role/TEST-ONLY-reader", secret_arn=SECRET,
    current_version=CURRENT, previous_version=PREVIOUS, unrelated_secret_arn=OTHER,
    reader_kind="admission", app_client_id="Iv23.test-client", installation_id="67890")
NONCE = "a123456789abcdef"
STORAGE = probes.Bindings(account="381183037896", region="us-east-1", nonce=NONCE, bucket="rcp-production-acceptance-" + NONCE,
    table="rcp-production-acceptance-" + NONCE)
INVENTORY = dict(current_version=CURRENT, previous_version=PREVIOUS,
    previous_stage_exists=True, unrelated_secret_exists=True)
PROOF = dict(repository_id="655073755", owner_id="483465", app_client_id="Iv23.test-client", installation_id="67890",
    actions="read", metadata="read", fixed_proof_read=True)

class ApiFailure(Exception):
    def __init__(self, code="AccessDeniedException", status=400):
        super().__init__("DO-NOT-LEAK-SECRET")
        self.response = {"Error": {"Code": code, "Message": "DO-NOT-LEAK-SECRET"},
                         "ResponseMetadata": {"HTTPStatusCode": status}}


class Secrets:
    def __init__(self, *, negative=None, version=CURRENT):
        self.negative = negative or ApiFailure()
        self.version = version
        self.calls = []
    def get_secret_value(self, **args):
        self.calls.append(args)
        if args == {"SecretId": SECRET, "VersionStage": "AWSCURRENT"}:
            return dict(ARN=SECRET, VersionId=self.version, VersionStages=["AWSCURRENT"],
                        SecretString="TEST-ONLY-SECRET-DO-NOT-LOG")
        raise self.negative


class Storage:
    def __init__(self):
        self.objects = {}
        self.calls = []
    def put_object(self, **args):
        self.calls.append(("put", args))
        if "IfNoneMatch" not in args:
            raise ApiFailure("AccessDenied", 403)
        if args["Key"] in self.objects:
            raise ApiFailure("PreconditionFailed", 412)
        self.objects[args["Key"]] = ("version-1", args["Body"])
        return dict(VersionId="version-1")
    def copy_object(self, **args):
        self.calls.append(("copy", args))
        assert args["AnnotationDirective"] == "EXCLUDE" and args["IfNoneMatch"] == "*"
        if args["Key"] in self.objects:
            raise ApiFailure("PreconditionFailed", 412)
        source = args["CopySource"]
        version, body = self.objects[source["Key"]]
        assert source["VersionId"] == version
        self.objects[args["Key"]] = ("version-2", body)
        return dict(VersionId="version-2")
    def get_object(self, **args):
        self.calls.append(("get", args))
        version, body = self.objects[args["Key"]]
        assert args["VersionId"] == version
        self.last_body = io.BytesIO(body)
        return dict(VersionId=version, Body=self.last_body, ContentLength=len(body))


class Transactions(DynamoTransactions):
    def __init__(self):
        super().__init__()
        self.api_calls = []
        self.items["policy"]["version"]["S"] = "acceptance-v1"
        self.items["budget#total"]["reserved"]["N"] = "0"
    def get_item(self, **args):
        self.api_calls.append(("get_item", copy.deepcopy(args)))
        response = super().get_item(**args)
        return {} if response.get("Item") is None else response
    def put_item(self, **args):
        self.api_calls.append(("put_item", copy.deepcopy(args)))
        raise ApiFailure()
    def transact_write_items(self, **args):
        self.api_calls.append(("transact_write_items", copy.deepcopy(args)))
        if len(args["TransactItems"]) == 1:
            raise ApiFailure()
        return super().transact_write_items(**args)


class BindingTests(unittest.TestCase):
    def test_default_bindings_fail_before_client_use(self):
        client = Mock()
        with self.assertRaises(probes.Hold): probes.reader_cases(probes.Bindings(), client, Mock(), fixture_inventory={})
        with self.assertRaises(probes.Hold): probes.storage_cases(probes.Bindings(), client)
        with self.assertRaises(probes.Hold): probes.transaction_cases(probes.Bindings(), client)
        self.assertEqual(client.mock_calls, [])
    def test_production_storage_and_malformed_bindings_rejected(self):
        for changes in [dict(bucket="rcp-production-benchmark"), dict(table="production"), dict(nonce="x"),
                        dict(bucket=STORAGE.bucket + "\n")]:
            with self.subTest(changes=changes), self.assertRaises(probes.Hold): replace(STORAGE, **changes).storage()
        for changes in [dict(secret_arn=SECRET.replace("381183037896", "123456789012")),
                        dict(region="us-west-2"), dict(previous_version=CURRENT), dict(reader_kind="operator"),
                        dict(app_client_id="bad client id"), dict(unrelated_secret_arn=SECRET), dict(role_arn="arn:aws:iam::381183037896:root")]:
            with self.subTest(changes=changes), self.assertRaises(probes.Hold): replace(READER, **changes).reader()
    def test_core_has_no_service_entrypoint_or_source_pin(self):
        self.assertFalse(hasattr(probes, "main"))
        self.assertFalse(hasattr(probes, "BASE_COMMIT"))
        self.assertFalse(hasattr(probes, "CANDIDATE"))
    def test_call_budget_and_expired_deadline_prevent_calls(self):
        client = Mock()
        now = [0.0]
        calls = probes.BoundedCalls({"s3": client}, clock=lambda: now[0])
        now[0] = 45
        with self.assertRaises(probes.Hold): calls.call("s3", "get_object", {})
        self.assertEqual(client.mock_calls, [])
        calls = probes.BoundedCalls({"s3": client}, clock=lambda: 0)
        for _ in range(probes.CASE_LIMIT): calls.call("s3", "get_object", {})
        with self.assertRaises(probes.Hold): calls.call("s3", "get_object", {})
        self.assertEqual(client.get_object.call_count, probes.CASE_LIMIT)
    def test_blocking_adapter_late_return_holds_without_retry(self):
        now = [0.0]
        client = Mock()
        def late(**args): now[0] = 46; return {}
        client.get_object.side_effect = late
        calls = probes.BoundedCalls({"s3": client}, clock=lambda: now[0])
        with self.assertRaisesRegex(probes.Hold, "requires reconciliation"): calls.call("s3", "get_object", {})
        self.assertEqual(client.get_object.call_count, 1)

    def test_late_denial_and_foreign_hold_never_escape_as_success_or_secret(self):
        now = [0.0]
        client = Mock()
        def late(**args):
            now[0] = 46
            raise ApiFailure()
        client.get_object.side_effect = late
        calls = probes.BoundedCalls({"s3": client}, clock=lambda: now[0])
        with self.assertRaisesRegex(probes.Hold, "requires reconciliation"):
            probes.denial(calls, "s3", "get_object", {})
        self.assertEqual(client.get_object.call_count, 1)
        secret = "DO-NOT-LEAK-adapter"
        client = Secrets()
        def foreign_hold(): raise probes.Hold(secret)
        with self.assertRaises(probes.Hold) as raised:
            probes.reader_cases(READER, client, foreign_hold, fixture_inventory=INVENTORY)
        self.assertNotIn(secret, str(raised.exception))


class ReaderTests(unittest.TestCase):
    def run_reader(self, secrets=None, proof=None, inventory=None):
        return probes.reader_cases(READER, secrets or Secrets(), lambda: PROOF if proof is None else proof,
            fixture_inventory=INVENTORY if inventory is None else inventory)
    def test_fixed_requests_and_sanitized_outcome(self):
        secrets = Secrets()
        result = self.run_reader(secrets)
        self.assertTrue(result["all_cases_observed"])
        self.assertTrue(result["probe_observations"])
        self.assertFalse(result["live_accepted"])
        self.assertEqual(result["calls"], 5)
        self.assertEqual(secrets.calls, [
            {"SecretId": SECRET, "VersionStage": "AWSCURRENT"},
            {"SecretId": SECRET},
            {"SecretId": SECRET, "VersionStage": "AWSPREVIOUS"},
            {"SecretId": SECRET, "VersionId": CURRENT},
            {"SecretId": OTHER, "VersionStage": "AWSCURRENT"},
        ])
        self.assertNotIn("SECRET", json.dumps(result))
        self.assertNotIn(SECRET, json.dumps(result))
        model = Session().get_service_model("secretsmanager")
        for request in secrets.calls:
            validate_parameters(request, model.operation_model("GetSecretValue").input_shape)
    def test_missing_fixture_is_unproven_not_iam_denial(self):
        for code in ["ResourceNotFoundException", "InvalidRequestException"]:
            with self.subTest(code=code):
                result = self.run_reader(Secrets(negative=ApiFailure(code, 400)))
                self.assertFalse(result["all_cases_observed"])
                self.assertEqual(result["cases"]["previous_stage"], "unproven")
    def test_positive_controls_required_before_any_request(self):
        client = Secrets()
        for inv in [{}, {**INVENTORY, "previous_stage_exists": False}, {**INVENTORY, "current_version": PREVIOUS}]:
            with self.subTest(inv=inv), self.assertRaises(probes.Hold): self.run_reader(client, inventory=inv)
        self.assertEqual(client.calls, [])
    def test_scope_or_current_version_drift_holds_before_negative_cases(self):
        client = Secrets(version=PREVIOUS)
        with self.assertRaises(probes.Hold): self.run_reader(client)
        self.assertEqual(len(client.calls), 1)
        for proof in [{**PROOF, "actions": "write"}, {**PROOF, "app_client_id": "98765"}, {**PROOF, "extra": "secret"}]:
            with self.subTest(proof=proof), self.assertRaises(probes.Hold): self.run_reader(proof=proof)
    def test_unexpected_and_transient_errors_are_sanitized_and_not_retried(self):
        for failure in [RuntimeError("DO-NOT-LEAK-SECRET"), ApiFailure("ThrottlingException", 429),
                        ApiFailure("AccessDeniedException", 500)]:
            client = Secrets(negative=failure)
            with self.subTest(failure=type(failure)), self.assertRaises(probes.Hold) as raised: self.run_reader(client)
            self.assertNotIn("DO-NOT-LEAK", str(raised.exception))
            self.assertEqual(len(client.calls), 2)
    def test_unexpected_allow_stops_before_more_probes(self):
        client = Mock()
        client.get_secret_value.side_effect = [Secrets().get_secret_value(SecretId=SECRET, VersionStage="AWSCURRENT"), {}]
        with self.assertRaisesRegex(probes.Hold, "negative operation succeeded"): self.run_reader(client)
        self.assertEqual(client.get_secret_value.call_count, 2)

    def test_final_reader_denial_after_deadline_is_a_hold(self):
        now = [0.0]
        client = Secrets()
        original = client.get_secret_value
        def delayed(**args):
            if args.get("SecretId") == OTHER:
                now[0] = 45.0
            return original(**args)
        client.get_secret_value = delayed
        with self.assertRaisesRegex(probes.Hold, "requires reconciliation"):
            probes.reader_cases(READER, client, lambda: PROOF, fixture_inventory=INVENTORY, clock=lambda: now[0])
        self.assertEqual(len(client.calls), 5)


class StorageTests(unittest.TestCase):
    def test_small_fixed_create_copy_version_reads_and_no_delete(self):
        client = Storage()
        result = probes.storage_cases(STORAGE, client)
        self.assertTrue(all(v == "pass" for v in result["cases"].values()))
        self.assertTrue(result["probe_observations"])
        self.assertFalse(result["live_accepted"])
        self.assertEqual(len(client.calls), 6)
        self.assertTrue(client.last_body.closed)
        self.assertEqual(set(client.objects), {"acceptance/source", "acceptance/copy"})
        self.assertTrue(all(call[1]["Bucket"] == STORAGE.bucket for call in client.calls))
    def test_truncated_copy_fails_and_closes_body(self):
        client = Storage()
        original = client.get_object
        def truncated(**args):
            value = original(**args)
            value["Body"].close()
            client.last_body = value["Body"] = io.BytesIO(probes.PAYLOAD[:-1])
            return value
        client.get_object = truncated
        with self.assertRaises(probes.Hold): probes.storage_cases(STORAGE, client)
        self.assertTrue(client.last_body.closed)
        self.assertEqual(len(client.calls), 5)
    def test_invalid_get_metadata_closes_body_before_hold(self):
        for changes in [{"VersionId": "foreign-version"}, {"ContentLength": len(probes.PAYLOAD) + 1}]:
            client = Storage()
            original = client.get_object
            def wrong(**args):
                value = original(**args)
                value.update(changes)
                return value
            client.get_object = wrong
            with self.subTest(changes=changes), self.assertRaises(probes.Hold): probes.storage_cases(STORAGE, client)
            self.assertTrue(client.last_body.closed)
            self.assertEqual(len(client.calls), 5)
    def test_read_error_closes_body_and_stops_without_more_calls(self):
        class BrokenBody(io.BytesIO):
            def read(self, size=-1): raise TimeoutError("DO-NOT-LEAK-SECRET")
        client = Storage()
        original = client.get_object
        def broken(**args):
            value = original(**args)
            value["Body"].close()
            client.last_body = value["Body"] = BrokenBody(probes.PAYLOAD)
            return value
        client.get_object = broken
        with self.assertRaises(probes.Hold) as raised: probes.storage_cases(STORAGE, client)
        self.assertNotIn("DO-NOT-LEAK", str(raised.exception))
        self.assertTrue(client.last_body.closed)
        self.assertEqual(len(client.calls), 5)
    def test_late_get_response_closes_body_before_ownership_transfer(self):
        now = [0.0]
        client = Storage()
        original = client.get_object
        def delayed(**args):
            value = original(**args)
            now[0] = 120.0
            return value
        client.get_object = delayed
        with self.assertRaisesRegex(probes.Hold, "requires reconciliation"):
            probes.storage_cases(STORAGE, client, clock=lambda: now[0])
        self.assertTrue(client.last_body.closed)
        self.assertEqual(len(client.calls), 5)

    def test_body_deadline_overrun_closes_and_holds_before_success(self):
        now = [0.0]
        class DelayedBody(io.BytesIO):
            def read(self, size=-1):
                now[0] = 120.0
                return super().read(size)
        client = Storage()
        original = client.get_object
        def delayed(**args):
            value = original(**args)
            value["Body"].close()
            client.last_body = value["Body"] = DelayedBody(probes.PAYLOAD)
            return value
        client.get_object = delayed
        with self.assertRaisesRegex(probes.Hold, "requires reconciliation"):
            probes.storage_cases(STORAGE, client, clock=lambda: now[0])
        self.assertTrue(client.last_body.closed)
        self.assertEqual(len(client.calls), 5)

    def test_nonversioned_bucket_and_uncertain_write_stop_without_retry(self):
        for response in [{}, {"VersionId": "null"}, {"VersionId": "a\nsecret"}]:
            client = Mock()
            client.put_object.return_value = response
            with self.subTest(response=response), self.assertRaises(probes.Hold): probes.storage_cases(STORAGE, client)
            self.assertEqual(client.put_object.call_count, 1)
        client = Mock();client.put_object.side_effect = TimeoutError("secret")
        with self.assertRaises(probes.Hold) as raised: probes.storage_cases(STORAGE, client)
        self.assertNotIn("secret", str(raised.exception))
        self.assertEqual(client.put_object.call_count, 1)
    def test_conditional_conflict_not_confused_with_authorization(self):
        calls = probes.BoundedCalls({"s3": Mock()})
        calls.clients["s3"].put_object.side_effect = ApiFailure("AccessDenied", 403)
        with self.assertRaises(probes.Hold): probes.denial(calls, "s3", "put_object", {}, condition=True)
        calls.clients["s3"].put_object.side_effect = ApiFailure("PreconditionFailed", 412)
        with self.assertRaises(probes.Hold): probes.denial(calls, "s3", "put_object", {}, condition=False)


class TransactionTests(unittest.TestCase):
    def test_exact_canonical_transaction_and_all_records_verified(self):
        client = Transactions()
        result = probes.transaction_cases(STORAGE, client)
        self.assertEqual([next(iter(i)) for i in probes.transaction_request(STORAGE)],
                         ["ConditionCheck", "Put", "Put", "Update", "Update", "Update"])
        self.assertEqual(result["calls"], 21)
        self.assertTrue(result["probe_observations"])
        self.assertFalse(result["live_accepted"])
        self.assertTrue(all(v == "pass" for v in result["cases"].values()))
        self.assertEqual(client.items["budget#total"]["reserved"], {"N": "1"})
    def test_nonempty_or_foreign_baseline_prevents_transaction(self):
        client = Transactions();client.items["active"] = {"pk": {"S": "active"}, "run_key": {"S": "other"}}
        with self.assertRaises(probes.Hold): probes.transaction_cases(STORAGE, client)
        self.assertEqual(client.writes, [])
    def test_missing_counter_update_is_detected(self):
        client = Transactions()
        def damage(): client.items["budget#total"]["reserved"] = {"N": "0"}
        client.after_commit = damage
        with self.assertRaises(probes.Hold): probes.transaction_cases(STORAGE, client)
        self.assertEqual(len(client.writes), 1)
        self.assertEqual(client.items["budget#total"]["reserved"], {"N": "0"})
    def test_condition_failure_is_not_iam_proof(self):
        client = Transactions();client.put_item = Mock(side_effect=ApiFailure("ConditionalCheckFailedException", 400))
        with self.assertRaises(probes.Hold): probes.transaction_cases(STORAGE, client)
    def test_ambiguous_transaction_is_never_retried(self):
        client = Transactions();client.after_commit = lambda: (_ for _ in ()).throw(TimeoutError("secret"))
        with self.assertRaises(probes.Hold) as raised: probes.transaction_cases(STORAGE, client)
        self.assertNotIn("secret", str(raised.exception))
        self.assertEqual(len(client.writes), 1)
        self.assertEqual(client.items["budget#total"]["reserved"], {"N": "1"})
        self.assertEqual(len(client.api_calls), 7)


class OidcTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.foreign = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(cls.key.public_key()))
        jwk.update(kid="offline-key", use="sig", alg="RS256")
        cls.jwks = json.dumps({"keys": [jwk]}).encode()
    def claims(self, **changes):
        value = dict(iss=auth.ISSUER, aud="rcp-benchmark-production-admission", sub="repo:wykurz/rcp:ref:refs/heads/main",
            exp=1800000300, iat=1800000000, nbf=1799999700, jti="offline-only", repository="wykurz/rcp",
            repository_id="655073755", repository_owner="wykurz", repository_owner_id="483465", ref="refs/heads/main",
            workflow_ref="wykurz/rcp/.github/workflows/aws-benchmark-production.yml@refs/heads/main",
            workflow_sha="d" * 40, sha="d" * 40, run_id="1", run_attempt="1",
            actor="wykurz", actor_id="483465", event_name="workflow_dispatch")
        value.update(changes);return value
    def observe(self, claims=None, key=None):
        verifier = auth.GitHubIdentityVerifier(repository="wykurz/rcp", repository_id="655073755",
            repository_owner_id="483465", audience="rcp-benchmark-production-admission",
            workflow_refs={self.claims()["workflow_ref"]}, clock=lambda: 1800000000, jwks_fetcher=lambda: self.jwks)
        token = jwt.encode(claims or self.claims(), key or self.key, algorithm="RS256", headers={"kid": "offline-key"})
        return probes.oidc_case(token, verifier)
    def test_real_verifier_positive_emits_no_token(self):
        outcome = self.observe()
        self.assertFalse(outcome["live_accepted"])
        self.assertEqual(outcome["lifetime_seconds"], 300)
        self.assertNotIn("token", json.dumps(outcome))
    def test_signed_claim_boundary_negatives(self):
        for change in [dict(ref="refs/heads/debug"), dict(ref="refs/tags/v1.0"), dict(event_name="pull_request"),
            dict(event_name="schedule"), dict(actor_id="999"), dict(run_attempt="2"),
            dict(repository_id="655073756"), dict(aud="sts.amazonaws.com"),
            dict(workflow_sha="a" * 40), dict(exp=1800000601), dict(exp=1800000000)]:
            with self.subTest(change=change), self.assertRaises(probes.Hold): self.observe(self.claims(**change))
    def test_signature_mismatch_fails_closed(self):
        with self.assertRaises(probes.Hold): self.observe(key=self.foreign)


class CoreContractTests(unittest.TestCase):
    def test_every_binding_input_type_fails_closed(self):
        for binding, fields, method in [
            (READER, ["account", "region", "role_arn", "secret_arn", "current_version", "previous_version",
                      "unrelated_secret_arn", "reader_kind", "app_client_id", "installation_id"], "reader"),
            (STORAGE, ["account", "region", "nonce", "bucket", "table"], "storage"),
        ]:
            for field in fields:
                for value in (None, True, 123, 1.5, [], {}, b"value"):
                    with self.subTest(field=field, value=value), self.assertRaises(probes.Hold):
                        getattr(replace(binding, **{field: value}), method)()

    def test_binding_objects_and_proof_adapter_are_required_before_calls(self):
        client = Mock()
        for binding in (None, {}, Mock()):
            for phase in (probes.storage_cases, probes.transaction_cases):
                with self.subTest(binding=type(binding), phase=phase.__name__), self.assertRaises(probes.Hold):
                    phase(binding, client)
            with self.assertRaises(probes.Hold):
                probes.reader_cases(binding, client, lambda: PROOF, fixture_inventory=INVENTORY)
        with self.assertRaises(probes.Hold):
            probes.reader_cases(READER, client, None, fixture_inventory=INVENTORY)
        self.assertEqual(client.mock_calls, [])

    def test_storage_account_and_region_are_required_before_any_call(self):
        for changes in [dict(account=""), dict(account="123"), dict(account="381183037896\n"),
                        dict(region=""), dict(region="us-west-2"), dict(region="us-east-1\n")]:
            for phase in (probes.storage_cases, probes.transaction_cases):
                client = Mock()
                with self.subTest(changes=changes, phase=phase.__name__), self.assertRaises(probes.Hold):
                    phase(replace(STORAGE, **changes), client)
                self.assertEqual(client.mock_calls, [])

    def test_reader_uses_provider_client_id_grammar(self):
        for value in ("Iv23.test-client", "Iv1.ab1112223334445c", "Iv23newOpaqueClientId"):
            self.assertEqual(replace(READER, app_client_id=value).reader().app_client_id, value)
        for value in ("", "bad value", "-prefix", "x" * 101, "client\nheader"):
            with self.subTest(value=value), self.assertRaises(probes.Hold):
                replace(READER, app_client_id=value).reader()

    def test_no_unlisted_operation_or_malformed_call_reaches_client(self):
        client = Mock()
        for service, operation, arguments in [
            ("s3", "delete_object", {}), ("dynamodb", "delete_item", {}),
            ("secrets", "put_secret_value", {}), ("sts", "get_caller_identity", {}),
            ([], "get_object", {}), ("s3", [], {}), ("s3", "get_object", []),
        ]:
            calls = probes.BoundedCalls({"s3": client, "dynamodb": client, "secrets": client})
            with self.subTest(service=service, operation=operation), self.assertRaises(probes.Hold):
                calls.call(service, operation, arguments)
            self.assertEqual(calls.calls, 0)
        self.assertEqual(client.mock_calls, [])

    def test_invalid_limits_and_clock_values_stop_before_calls(self):
        client = Mock()
        for seconds in (0, -1, 121, True, 1.0, "45", None):
            with self.subTest(seconds=seconds), self.assertRaises(probes.Hold):
                probes.BoundedCalls({"s3": client}, seconds=seconds)
        for now in (float("nan"), float("inf"), float("-inf"), True, "0", None):
            with self.subTest(now=now), self.assertRaises(probes.Hold):
                probes.BoundedCalls({"s3": client}, clock=lambda: now)
        clock = Mock(side_effect=[1, 0])
        calls = probes.BoundedCalls({"s3": client}, clock=clock)
        with self.assertRaises(probes.Hold): calls.call("s3", "get_object", {})
        self.assertEqual(client.mock_calls, [])

    def test_post_call_clock_failure_closes_stream_and_redacts_exception(self):
        for failure in (float("nan"), RuntimeError("DO-NOT-LEAK-SECRET")):
            body = io.BytesIO(probes.PAYLOAD)
            client = Mock()
            client.get_object.return_value = {"Body": body}
            clock = Mock(side_effect=[0, 0, failure])
            calls = probes.BoundedCalls({"s3": client}, clock=clock)
            with self.subTest(failure=type(failure)), self.assertRaises(probes.Hold) as raised:
                calls.call("s3", "get_object", {})
            self.assertTrue(body.closed)
            self.assertEqual(client.get_object.call_count, 1)
            self.assertNotIn("DO-NOT-LEAK", "".join(traceback.format_exception(raised.exception)))

    def test_service_denial_pairs_are_exact_and_missing_fixtures_are_unproven(self):
        cases = [
            ("secrets", "get_secret_value", "AccessDeniedException", 400, "pass"),
            ("dynamodb", "put_item", "AccessDeniedException", 400, "pass"),
            ("s3", "put_object", "AccessDenied", 403, "pass"),
            ("secrets", "get_secret_value", "ResourceNotFoundException", 400, "unproven"),
            ("secrets", "get_secret_value", "InvalidRequestException", 400, "unproven"),
            ("dynamodb", "put_item", "ResourceNotFoundException", 400, "unproven"),
            ("s3", "put_object", "NoSuchKey", 404, "unproven"),
            ("s3", "put_object", "NoSuchVersion", 404, "unproven"),
        ]
        for service, operation, code, status, expected in cases:
            client = Mock()
            getattr(client, operation).side_effect = ApiFailure(code, status)
            calls = probes.BoundedCalls({service: client})
            with self.subTest(service=service, code=code):
                self.assertEqual(probes.denial(calls, service, operation, {}), expected)
                self.assertEqual(calls.calls, 1)
        for service, operation, code, status in [
            ("secrets", "get_secret_value", "AccessDenied", 403),
            ("secrets", "get_secret_value", "AccessDeniedException", 403),
            ("s3", "put_object", "AccessDeniedException", 400),
            ("s3", "put_object", "AccessDenied", 400),
            ("dynamodb", "put_item", "AccessDeniedException", 500),
            ("dynamodb", "put_item", "ConditionalCheckFailedException", 400),
            ("secrets", "get_secret_value", "ResourceNotFoundException", 500),
            ("secrets", "get_secret_value", "NoSuchKey", 404),
        ]:
            client = Mock()
            getattr(client, operation).side_effect = ApiFailure(code, status)
            calls = probes.BoundedCalls({service: client})
            with self.subTest(service=service, code=code, status=status), self.assertRaises(probes.Hold):
                probes.denial(calls, service, operation, {})
            self.assertEqual(calls.calls, 1)

    def test_malformed_error_envelopes_are_sanitized_holds(self):
        for response in (None, [], {}, {"Error": None}, {"Error": []},
                         {"Error": {"Code": "AccessDeniedException"}, "ResponseMetadata": None},
                         {"Error": {"Code": []}, "ResponseMetadata": {"HTTPStatusCode": 400}},
                         {"Error": {"Code": "AccessDeniedException"}, "ResponseMetadata": {"HTTPStatusCode": "400"}},
                         {"Error": {"Code": "AccessDeniedException"}, "ResponseMetadata": {"HTTPStatusCode": 400.0}}):
            error = ApiFailure()
            error.response = response
            client = Mock()
            client.get_secret_value.side_effect = error
            calls = probes.BoundedCalls({"secrets": client})
            with self.subTest(response=response), self.assertRaises(probes.Hold) as raised:
                probes.denial(calls, "secrets", "get_secret_value", {})
            self.assertNotIn("DO-NOT-LEAK", "".join(traceback.format_exception(raised.exception)))
            self.assertEqual(calls.calls, 1)

    def test_malformed_positive_reader_response_stops_before_proof(self):
        valid = Secrets().get_secret_value(SecretId=SECRET, VersionStage="AWSCURRENT")
        changes = [{"VersionStages": value} for value in (None, "AWSCURRENT", {}, [],
            ["AWSCURRENT", "AWSCURRENT"], ["AWSCURRENT", "AWSPREVIOUS"], ["AWSCURRENT", 1])]
        changes += [{"SecretString": value} for value in (None, "", b"secret", "x" * 16385)]
        changes += [{"SecretBinary": b"secret"}, {"ARN": OTHER}]
        for response in [None, []] + [{**valid, **change} for change in changes]:
            client, proof = Mock(), Mock()
            client.get_secret_value.return_value = response
            with self.subTest(response_type=type(response)), self.assertRaises(probes.Hold):
                probes.reader_cases(READER, client, proof, fixture_inventory=INVENTORY)
            self.assertEqual(client.get_secret_value.call_count, 1)
            proof.assert_not_called()

    def test_inventory_and_proof_require_boolean_evidence(self):
        for field in ("previous_stage_exists", "unrelated_secret_exists"):
            client = Secrets()
            with self.subTest(field=field), self.assertRaises(probes.Hold):
                probes.reader_cases(READER, client, lambda: PROOF, fixture_inventory={**INVENTORY, field: 1})
            self.assertEqual(client.calls, [])
        client = Secrets()
        with self.assertRaises(probes.Hold):
            probes.reader_cases(READER, client, lambda: {**PROOF, "fixed_proof_read": 1}, fixture_inventory=INVENTORY)
        self.assertEqual(len(client.calls), 1)

    def test_late_proof_holds_before_negative_reader_requests(self):
        now, client = [0.0], Secrets()
        def proof():
            now[0] = 45.0
            return PROOF
        with self.assertRaisesRegex(probes.Hold, "requires reconciliation"):
            probes.reader_cases(READER, client, proof, fixture_inventory=INVENTORY, clock=lambda: now[0])
        self.assertEqual(len(client.calls), 1)

    def test_s3_exact_fixed_requests_and_sdk_shapes(self):
        client = Storage()
        probes.storage_cases(STORAGE, client)
        put = dict(Bucket=STORAGE.bucket, Key="acceptance/source", Body=probes.PAYLOAD,
            IfNoneMatch="*", ExpectedBucketOwner=STORAGE.account, ServerSideEncryption="AES256", ChecksumAlgorithm="SHA256",
            ContentType="application/octet-stream")
        copied = dict(Bucket=STORAGE.bucket, Key="acceptance/copy", IfNoneMatch="*",
            ExpectedBucketOwner=STORAGE.account, ExpectedSourceBucketOwner=STORAGE.account,
            CopySource={"Bucket": STORAGE.bucket, "Key": "acceptance/source", "VersionId": "version-1"},
            ServerSideEncryption="AES256", ChecksumAlgorithm="SHA256", AnnotationDirective="EXCLUDE",
            MetadataDirective="REPLACE", ContentType="application/octet-stream")
        unrestricted = {k: v for k, v in put.items() if k != "IfNoneMatch"}
        self.assertEqual(client.calls, [("put", put), ("put", put), ("copy", copied), ("copy", copied),
            ("get", {"Bucket": STORAGE.bucket, "Key": "acceptance/copy", "VersionId": "version-2",
                     "ExpectedBucketOwner": STORAGE.account}),
            ("put", unrestricted)])
        model = Session().get_service_model("s3")
        for operation, request in client.calls:
            name = {"put": "PutObject", "copy": "CopyObject", "get": "GetObject"}[operation]
            wire_request = copy.deepcopy(request)
            if name == "CopyObject": handle_copy_source_param(wire_request)
            validate_parameters(wire_request, model.operation_model(name).input_shape)

    def test_malformed_s3_body_types_and_close_failures_stop_after_read(self):
        class Body:
            def __init__(self, value, close_error=False):
                self.value, self.close_error, self.closed, self.reads = value, close_error, False, []
            def read(self, size):
                self.reads.append(size)
                return self.value
            def close(self):
                self.closed = True
                if self.close_error: raise RuntimeError("DO-NOT-LEAK-SECRET")
        for value, length, close_error in [(None, len(probes.PAYLOAD), False),
                (bytearray(probes.PAYLOAD), len(probes.PAYLOAD), False),
                (probes.PAYLOAD, float(len(probes.PAYLOAD)), False),
                (probes.PAYLOAD + b"extra", len(probes.PAYLOAD), False),
                (probes.PAYLOAD, len(probes.PAYLOAD), True)]:
            client, body = Storage(), Body(value, close_error)
            original = client.get_object
            def get(**args):
                response = original(**args)
                response["Body"].close()
                response.update(Body=body, ContentLength=length)
                return response
            client.get_object = get
            with self.subTest(value_type=type(value), close_error=close_error), self.assertRaises(probes.Hold) as raised:
                probes.storage_cases(STORAGE, client)
            self.assertTrue(body.closed)
            self.assertIn(body.reads, ([], [len(probes.PAYLOAD) + 1]))
            self.assertEqual(len(client.calls), 5)
            self.assertNotIn("DO-NOT-LEAK", str(raised.exception))

    def test_unexpected_write_permission_holds_without_more_probes(self):
        storage = Storage()
        original_put = storage.put_object
        def allow_unconditional(**args):
            if "IfNoneMatch" not in args:
                storage.calls.append(("put", args))
                return {"VersionId": "unexpected"}
            return original_put(**args)
        storage.put_object = allow_unconditional
        with self.assertRaisesRegex(probes.Hold, "negative operation succeeded"):
            probes.storage_cases(STORAGE, storage)
        self.assertEqual(len(storage.calls), 6)
        transactions = Transactions()
        def allow_direct(**args):
            transactions.api_calls.append(("put_item", args))
            return {}
        transactions.put_item = allow_direct
        with self.assertRaisesRegex(probes.Hold, "negative operation succeeded"):
            probes.transaction_cases(STORAGE, transactions)
        self.assertEqual(len(transactions.api_calls), 14)
        self.assertEqual(len(transactions.writes), 1)

    def test_final_s3_denial_after_deadline_holds(self):
        now, client = [0.0], Storage()
        original_put = client.put_object
        def late(**args):
            if "IfNoneMatch" not in args: now[0] = 120.0
            return original_put(**args)
        client.put_object = late
        with self.assertRaisesRegex(probes.Hold, "requires reconciliation"):
            probes.storage_cases(STORAGE, client, clock=lambda: now[0])
        self.assertEqual(len(client.calls), 6)
        self.assertTrue(client.last_body.closed)

    def test_transaction_exact_negative_contracts_consistent_reads_and_sdk_shapes(self):
        client = Transactions()
        probes.transaction_cases(STORAGE, client)
        keys = ["policy", "run#repo-655073755-run-1", "active", "budget#total",
                "budget#month#2026-10", "rate#day#2026-10-27"]
        reads = [request for operation, request in client.api_calls if operation == "get_item"]
        self.assertEqual(reads, [dict(TableName=STORAGE.table, Key={"pk": {"S": key}}, ConsistentRead=True)
                                for key in keys] * 3)
        operation, positive = client.api_calls[6]
        self.assertEqual(operation, "transact_write_items")
        self.assertEqual(set(positive), {"TransactItems", "ClientRequestToken"})
        self.assertEqual(positive["ClientRequestToken"], "acceptance-" + NONCE)
        self.assertEqual(positive["TransactItems"], probes.transaction_request(STORAGE))
        self.assertEqual(client.api_calls[13], ("put_item", dict(TableName=STORAGE.table,
            Item=positive["TransactItems"][1]["Put"]["Item"], ConditionExpression="attribute_exists(pk)")))
        self.assertEqual(client.api_calls[14], ("transact_write_items", dict(ClientRequestToken="negative-" + NONCE,
            TransactItems=[{"Update": {"TableName": STORAGE.table, "Key": {"pk": {"S": keys[1]}},
                "UpdateExpression": "SET #field = :value", "ExpressionAttributeNames": {"#field": "fingerprint"},
                "ExpressionAttributeValues": {":value": {"S": "c" * 64}}}}])))
        model = Session().get_service_model("dynamodb")
        for operation, request in client.api_calls:
            name = {"get_item": "GetItem", "put_item": "PutItem", "transact_write_items": "TransactWriteItems"}[operation]
            validate_parameters(request, model.operation_model(name).input_shape)

    def test_malformed_transaction_read_outputs_hold_before_any_write(self):
        for response in (None, [], {"Item": None}, {"Item": {}}, {"Item": {"pk": {"S": "policy"},
                "version": {"S": "acceptance-v1"}, "enabled": {"BOOL": 1}}}):
            client = Transactions()
            client.get_item = Mock(return_value=response)
            with self.subTest(response=response), self.assertRaises(probes.Hold):
                probes.transaction_cases(STORAGE, client)
            self.assertEqual(client.writes, [])
            self.assertEqual(client.get_item.call_count, 1)

    def test_transaction_malformed_write_return_holds_without_retry_or_readback(self):
        client = Transactions()
        original = client.transact_write_items
        def write(**args):
            original(**args)
            return None
        client.transact_write_items = write
        with self.assertRaises(probes.Hold): probes.transaction_cases(STORAGE, client)
        self.assertEqual(len(client.writes), 1)
        self.assertEqual(len(client.api_calls), 7)
        self.assertEqual(client.items["budget#total"]["reserved"], {"N": "1"})

    def test_changed_records_after_denials_and_final_late_read_never_pass(self):
        client = Transactions()
        original = client.put_item
        def damaged(**args):
            client.items["budget#total"]["reserved"] = {"N": "2"}
            return original(**args)
        client.put_item = damaged
        with self.assertRaises(probes.Hold): probes.transaction_cases(STORAGE, client)
        self.assertEqual(len(client.api_calls), 21)
        now, client = [0.0], Transactions()
        original = client.get_item
        def late(**args):
            response = original(**args)
            if len(client.api_calls) == 21: now[0] = 120.0
            return response
        client.get_item = late
        with self.assertRaisesRegex(probes.Hold, "requires reconciliation"):
            probes.transaction_cases(STORAGE, client, clock=lambda: now[0])
        self.assertEqual(len(client.api_calls), 21)
        self.assertEqual(len(client.writes), 1)

if __name__ == "__main__": unittest.main()
