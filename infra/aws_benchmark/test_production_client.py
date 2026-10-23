import base64
import io
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError, ReadTimeoutError
import urllib.error

from . import production_client as client


FUNCTION = "arn:aws:lambda:us-east-1:123456789012:function:admission"
EXECUTION = "arn:aws:states:us-east-1:123456789012:execution:production:100-1"
NOW = 1800000000


def receipt(**values):
    return dict(schema_version=1, run_key="repo-123-run-100", epoch="a" * 64,
                state="awaiting-inputs", run="100-1",
                upload_deadline=datetime.fromtimestamp(NOW + 600, timezone.utc).isoformat()) | values


def response(value):
    return dict(StatusCode=200, Payload=io.BytesIO(json.dumps(dict(ok=True, result=value)).encode()))


class ProductionClientTests(unittest.TestCase):
    def submit(self, *args, **kwargs):
        options = dict(expected_run="100-1", clock=lambda: NOW, wall_clock=lambda: NOW, sleep=Mock()) | kwargs
        return client.submit(*args, **options)

    def test_prepares_identical_aa_bytes_without_executing_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("rcp", "rcpd", "filegen"):
                (root / name).write_bytes(b"not executable " + name.encode())
            manifest, binaries = client.prepare(root, "a" * 40)
        self.assertEqual(manifest["baseline"], manifest["candidate"])
        self.assertEqual(manifest["suite"], "bulk-1g-metadata")
        for name in ("rcp", "rcpd", "filegen"):
            self.assertEqual(binaries["baseline/" + name], binaries["candidate/" + name])
            self.assertEqual(manifest["binaries"]["baseline/" + name], manifest["binaries"]["candidate/" + name])

    def test_rejects_symlink_artifact_without_reading_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "private").write_bytes(b"secret")
            (root / "rcp").symlink_to("private")
            with self.assertRaises(OSError):
                client.prepare(root, "a" * 40)

    def test_rejects_empty_or_non_sha_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("rcp", "rcpd", "filegen"):
                (root / name).write_bytes(b"")
            with self.assertRaises(ValueError):
                client.prepare(root, "a" * 40)
            with self.assertRaises(ValueError):
                client.prepare(root, "main")

    def test_rejects_oversized_binary_before_cloud_calls(self):
        from . import controller
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "rcp").write_bytes(b"12345")
            with patch.object(controller, "MAX_BINARY", 4), self.assertRaisesRegex(ValueError, "invalid binary"):
                client.prepare(root, "a" * 40)

    def test_never_exports_capabilities_or_unknown_fields(self):
        value = receipt(uploads={"x": "secret-url"}, token="secret-token", credentials="secret")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            client.persist(path, value)
            text = path.read_text()
            self.assertNotIn("secret", text)
            self.assertNotIn("uploads", text)
            self.assertEqual(json.loads(text)["run_key"], value["run_key"])

    def test_exhausted_same_capability_upload_retries_retain_receipt_without_seal(self):
        api, uploader, token = Mock(), Mock(side_effect=TimeoutError("sensitive URL")), Mock(return_value="sensitive JWT")
        inputs = {name: b"bytes" for name in client.BINARIES}
        value = receipt(uploads={name: {} for name in client.BINARIES})
        api.invoke.return_value = response(value)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            with self.assertRaisesRegex(ValueError, "reconciliation remains pending"):
                self.submit(api, FUNCTION, {}, inputs, path, audience="admission", bucket="bucket", region="region",
                              token_reader=token, uploader=uploader)
            self.assertTrue(path.exists())
            self.assertNotIn("sensitive", path.read_text())
        self.assertEqual(api.invoke.call_count, 1)
        self.assertEqual(uploader.call_count, 4)

    def test_pending_closed_and_rerun_receipts_fail_without_upload_or_seal(self):
        for value in (receipt(), receipt(state="closing"), receipt(state="retired"),
                      receipt(state="armed", run="99-1")):
            api, uploader = Mock(), Mock()
            api.invoke.return_value = response(value)
            with self.subTest(state=value["state"]), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "receipt.json"
                with self.assertRaises(ValueError):
                    self.submit(api, FUNCTION, {}, {}, path, audience="admission", bucket="bucket",
                                region="us-east-1", token_reader=lambda _: "token", uploader=uploader)
                self.assertEqual(json.loads(path.read_text())["state"], value["state"])
            self.assertEqual(api.invoke.call_count, 1)
            uploader.assert_not_called()

    def test_uploads_all_six_then_seals_using_a_fresh_signed_token(self):
        api, uploader, token = Mock(), Mock(), Mock(side_effect=["begin-token", "seal-token"])
        api.invoke.side_effect = [response(receipt(uploads={name: {} for name in client.BINARIES})),
                                  response(receipt(state="armed", execution_arn=EXECUTION, execution_status="submitted"))]
        with tempfile.TemporaryDirectory() as directory:
            result = self.submit(api, FUNCTION, {}, {name: b"x" for name in client.BINARIES},
                Path(directory) / "receipt.json", audience="admission", bucket="bucket", region="region",
                token_reader=token, uploader=uploader)
        self.assertEqual(result["execution_status"], "submitted")
        self.assertEqual(uploader.call_count, 6)
        self.assertEqual(token.call_count, 2)
        payloads = [json.loads(call.kwargs["Payload"]) for call in api.invoke.call_args_list]
        self.assertEqual([p["action"] for p in payloads], ["begin", "seal"])
        self.assertEqual([p["token"] for p in payloads], ["begin-token", "seal-token"])

    def test_lost_begin_and_seal_responses_reconcile_same_attempt_with_fresh_tokens(self):
        api, uploader = Mock(), Mock()
        transient = ReadTimeoutError(endpoint_url="https://private.invalid")
        api.invoke.side_effect = [transient, response(receipt(uploads={name: {} for name in client.BINARIES})),
            transient, response(receipt(state="armed", execution_arn=EXECUTION, execution_status="RUNNING"))]
        tokens = Mock(side_effect=["begin-one", "begin-two", "seal-one", "seal-two"])
        sleeps = Mock()
        with tempfile.TemporaryDirectory() as directory:
            result = self.submit(api, FUNCTION, {"pinned": True}, {name: b"x" for name in client.BINARIES},
                Path(directory) / "receipt.json", audience="admission", bucket="bucket", region="us-east-1",
                token_reader=tokens, uploader=uploader, sleep=sleeps)
        self.assertEqual(result["execution_status"], "RUNNING")
        payloads = [json.loads(call.kwargs["Payload"]) for call in api.invoke.call_args_list]
        self.assertEqual([value["action"] for value in payloads], ["begin", "begin", "seal", "seal"])
        self.assertEqual(payloads[0]["manifest"], payloads[1]["manifest"])
        self.assertEqual(tokens.call_count, 4)
        self.assertEqual(sleeps.call_count, 2)
        self.assertEqual(uploader.call_count, 6)

    def test_armed_begin_requires_exact_execution_reconciliation(self):
        api = Mock()
        api.invoke.side_effect = [response(receipt(state="armed")),
                                 response(receipt(state="armed", execution_arn=EXECUTION, execution_status="RUNNING"))]
        uploader = Mock()
        with tempfile.TemporaryDirectory() as directory:
            result = self.submit(api, FUNCTION, {}, {}, Path(directory) / "receipt.json", audience="admission",
                bucket="bucket", region="us-east-1", token_reader=lambda _: "token", uploader=uploader)
        self.assertEqual(result["execution_arn"], EXECUTION)
        uploader.assert_not_called()
        self.assertEqual(api.invoke.call_count, 2)

    def test_immediate_coordinator_progress_after_seal_is_verified_submission(self):
        for phase in ("launching", "uncertain", "requested"):
            api = Mock()
            api.invoke.side_effect = [response(receipt(uploads={name: {} for name in client.BINARIES})),
                response(receipt(state=phase, execution_arn=EXECUTION, execution_status="submitted"))]
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                result = self.submit(api, FUNCTION, {}, {name: b"x" for name in client.BINARIES},
                    Path(directory) / "receipt.json", audience="admission", bucket="bucket", region="us-east-1",
                    token_reader=lambda _: "token", uploader=Mock())
            self.assertEqual(result["state"], phase)
            self.assertEqual(api.invoke.call_count, 2)

    def test_lost_seal_response_and_new_begin_reconcile_active_progress(self):
        for initial in (receipt(state="armed"), receipt(state="requested")):
            api = Mock()
            api.invoke.side_effect = [response(initial), ReadTimeoutError(endpoint_url="https://private.invalid"),
                response(receipt(state="requested", execution_arn=EXECUTION, execution_status="RUNNING"))]
            with self.subTest(initial=initial["state"]), tempfile.TemporaryDirectory() as directory:
                result = self.submit(api, FUNCTION, {}, {}, Path(directory) / "receipt.json", audience="admission",
                    bucket="bucket", region="us-east-1", token_reader=lambda _: "token", uploader=Mock())
            self.assertEqual(result["state"], "requested")
            self.assertEqual([json.loads(call.kwargs["Payload"])["action"] for call in api.invoke.call_args_list],
                             ["begin", "seal", "seal"])

    def test_terminal_wrong_execution_or_failed_seal_is_not_success(self):
        for value in (receipt(state="closing"), receipt(state="retired"),
                receipt(state="armed", execution_arn=EXECUTION.replace("100-1", "101-1"), execution_status="RUNNING"),
                receipt(state="armed", execution_arn=EXECUTION, execution_status="FAILED")):
            api = Mock()
            api.invoke.side_effect = [response(receipt(state="armed")), response(value)]
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "receipt.json"
                with self.assertRaises(ValueError):
                    self.submit(api, FUNCTION, {}, {}, path, audience="admission", bucket="bucket",
                        region="us-east-1", token_reader=lambda _: "token")
                self.assertEqual(json.loads(path.read_text())["state"], value["state"])

    def test_transient_envelope_and_throttle_retry_but_access_denial_does_not(self):
        delay = dict(StatusCode=200, Payload=io.BytesIO(json.dumps(
            dict(ok=False, error="admission_retryable", retry_after_seconds=60)).encode()))
        api = Mock()
        api.invoke.side_effect = [delay, response(receipt(state="armed"))]
        clock = [NOW]
        sleeps = []
        def sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds
        result = client.reconcile(lambda: client.invoke(api, FUNCTION, {}), deadline=NOW + 120,
                                  reserve=10, clock=lambda: clock[0], sleep=sleep)
        self.assertEqual(result["state"], "armed")
        self.assertEqual(sleeps, [60])
        for code, expected in (("TooManyRequestsException", 4), ("AccessDeniedException", 1)):
            call = Mock(side_effect=ClientError({"Error": {"Code": code}}, "Invoke"))
            with self.subTest(code=code), self.assertRaises(Exception):
                client.reconcile(call, deadline=NOW + 600, reserve=60, clock=lambda: NOW, sleep=Mock())
            self.assertEqual(call.call_count, expected)

    def test_retry_and_upload_deadlines_cannot_be_extended(self):
        operation = Mock(side_effect=client.AdmissionRetryable(3600))
        sleep = Mock()
        with self.assertRaisesRegex(ValueError, "reconciliation remains pending"):
            client.reconcile(operation, deadline=NOW + 600, reserve=60, clock=lambda: NOW, sleep=sleep)
        self.assertEqual(operation.call_count, 1)
        sleep.assert_not_called()
        api, uploader = Mock(), Mock()
        api.invoke.return_value = response(receipt(uploads={name: {} for name in client.BINARIES},
            upload_deadline=datetime.fromtimestamp(NOW + 20, timezone.utc).isoformat()))
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(ValueError, "window exhausted"):
            self.submit(api, FUNCTION, {}, {}, Path(directory) / "receipt.json", audience="admission",
                bucket="bucket", region="us-east-1", token_reader=lambda _: "token", uploader=uploader)
        uploader.assert_not_called()
        self.assertEqual(api.invoke.call_count, 1)

    def test_lambda_transport_outlives_handler_and_has_no_hidden_sdk_retries(self):
        config = client.lambda_config()
        import yaml
        template = yaml.safe_load(Path(__file__).with_name("production-stack.yaml").read_text())
        handler_timeout = template["Resources"]["AdmissionFunction"]["Properties"]["Timeout"]
        self.assertGreater(config.read_timeout, handler_timeout)
        self.assertGreaterEqual(client.INVOKE_RESERVE_SECONDS,
            client.TOKEN_READ_TIMEOUT + config.connect_timeout + config.read_timeout)
        self.assertEqual(config.retries, {"total_max_attempts": 1})
        self.assertEqual(config.proxies, {})
        self.assertTrue(config.ignore_configured_endpoint_urls)

    def test_function_error_retries_exact_payload_and_closes_untrusted_body(self):
        api = Mock()
        body = io.BytesIO(b'sensitive timeout payload')
        api.invoke.side_effect = [dict(StatusCode=200, FunctionError="Unhandled", Payload=body),
                                  response(receipt(state="armed"))]
        self.assertEqual(client.reconcile(lambda: client.invoke(api, FUNCTION, {"action": "seal"}),
            deadline=NOW + 120, reserve=10, clock=lambda: NOW, sleep=Mock())["state"], "armed")
        self.assertTrue(body.closed)
        self.assertEqual(api.invoke.call_args_list[0], api.invoke.call_args_list[1])

    def test_upload_request_timeout_replays_same_capability_but_other_400_is_permanent(self):
        data = b"test"
        capability = dict(url="https://bucket.s3.amazonaws.com/object", headers={
            "Content-Length": "4", "x-amz-checksum-sha256": base64.b64encode(client.hashlib.sha256(data).digest()).decode(),
            "If-None-Match": "*", "Content-Type": "application/octet-stream", "x-amz-server-side-encryption": "AES256"})
        for body, calls in ((b"<Error><Code>RequestTimeout</Code></Error>", 4),
                (b"<Error><Code>AccessDenied</Code></Error>", 1), (b"invalid", 1),
                (b"<!DOCTYPE Error><Error><Code>RequestTimeout</Code></Error>", 1), (b"x" * 4097, 1)):
            with self.subTest(body=body[:40]), patch.object(client, "opener") as opener:
                bodies = []
                def fail(*args, **kwargs):
                    stream = io.BytesIO(body)
                    bodies.append(stream)
                    raise urllib.error.HTTPError("sensitive URL", 400, "bad", {}, stream)
                opener.return_value.open.side_effect = fail
                with self.assertRaises(ValueError) as caught:
                    client.reconcile(lambda: client.upload(capability, data, bucket="bucket", region="us-east-1"),
                        deadline=NOW + 600, reserve=30, clock=lambda: NOW, sleep=Mock())
                self.assertEqual(opener.return_value.open.call_count, calls)
                self.assertTrue(all(stream.closed for stream in bodies))
                self.assertNotIn("sensitive", str(caught.exception))
                requests = [call.args[0] for call in opener.return_value.open.call_args_list]
                self.assertTrue(all(request.full_url == capability["url"] and request.data == data for request in requests))

    def test_tls_and_http_transport_failures_retry_without_retrying_bad_certificates(self):
        for error, count in ((client.ssl.SSLError("connection interrupted"), 4),
                (client.http.client.IncompleteRead(b"private"), 4),
                (client.http.client.RemoteDisconnected("private"), 4),
                (client.ssl.SSLCertVerificationError("private"), 1),
                (urllib.error.URLError(client.ssl.SSLCertVerificationError("private")), 1)):
            operation = Mock(side_effect=error)
            with self.subTest(kind=type(error).__name__), self.assertRaises(Exception):
                client.reconcile(operation, deadline=NOW + 600, reserve=30, clock=lambda: NOW, sleep=Mock())
            self.assertEqual(operation.call_count, count)

    def test_rejects_service_errors_and_oversized_responses(self):
        api = Mock()
        for value in (dict(StatusCode=200, FunctionError="Unhandled"),
                      dict(StatusCode=500),
                      dict(StatusCode=200, Payload=io.BytesIO(b" " * (client.MAX_RESPONSE + 1))),
                      dict(StatusCode=200, Payload=io.BytesIO(b'{"ok":false,"error":"no"}'))):
            api.invoke.return_value = value
            with self.subTest(value=list(value)), self.assertRaises(ValueError):
                client.invoke(api, "function", {})

    def test_upload_rejects_redirect_destination_and_changed_bytes(self):
        data = b"test"
        headers = {"Content-Length": "4", "x-amz-checksum-sha256": base64.b64encode(client.hashlib.sha256(data).digest()).decode(),
                   "If-None-Match": "*", "Content-Type": "application/octet-stream", "x-amz-server-side-encryption": "AES256"}
        for url in ("https://evil.example/object", "http://bucket.s3.amazonaws.com/object",
                    "https://user:pass@bucket.s3.amazonaws.com/object", "https://bucket.s3.amazonaws.com/object#x"):
            with self.subTest(url=url), self.assertRaises(ValueError), patch.object(client, "opener") as opener:
                client.upload(dict(url=url, headers=headers), data, bucket="bucket", region="us-east-1")
                opener.assert_not_called()
        with self.assertRaises(ValueError):
            client.upload(dict(url="https://bucket.s3.amazonaws.com/object", headers=headers), b"bad!", bucket="bucket", region="us-east-1")

    def test_create_only_conflict_is_deferred_to_server_checksum_validation(self):
        data = b"test"
        headers = {"Content-Length": "4", "x-amz-checksum-sha256": base64.b64encode(client.hashlib.sha256(data).digest()).decode(),
                   "If-None-Match": "*", "Content-Type": "application/octet-stream", "x-amz-server-side-encryption": "AES256"}
        with patch.object(client, "opener") as opener:
            opener.return_value.open.side_effect = urllib.error.HTTPError("private", 412, "exists", {}, None)
            client.upload(dict(url="https://bucket.s3.amazonaws.com/object", headers=headers), data, bucket="bucket", region="us-east-1")
        self.assertEqual(opener.return_value.open.call_count, 1)

    def test_token_request_does_not_follow_redirects_and_bounds_response(self):
        with self.assertRaises(ValueError):
            client.NoRedirect().redirect_request(None, None, 302, "", {}, "https://evil.example")
        env = dict(ACTIONS_ID_TOKEN_REQUEST_URL="https://github.example/id?request=1", ACTIONS_ID_TOKEN_REQUEST_TOKEN="secret")
        result = Mock(status=200)
        result.read.return_value = json.dumps(dict(value="signed-token")).encode()
        with patch.dict(os.environ, env), patch.object(client, "opener") as opener:
            opener.return_value.open.return_value.__enter__.return_value = result
            self.assertEqual(client.token("rcp-admission"), "signed-token")
            call = opener.return_value.open.call_args
            self.assertIn("audience=rcp-admission", call.args[0].full_url)
            self.assertEqual(call.args[0].get_header("Authorization"), "Bearer secret")
            result.read.return_value = b" " * (client.MAX_RESPONSE + 1)
            with self.assertRaises(ValueError):
                client.token("rcp-admission")


if __name__ == "__main__":
    unittest.main()
