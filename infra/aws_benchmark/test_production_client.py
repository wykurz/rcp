import base64
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import urllib.error

from . import production_client as client


def receipt(**values):
    return dict(schema_version=1, run_key="repo-123-run-100", epoch="a" * 64,
                state="awaiting-inputs", run="100-1", **values)


def response(value):
    return dict(StatusCode=200, Payload=io.BytesIO(json.dumps(dict(ok=True, result=value)).encode()))


class ProductionClientTests(unittest.TestCase):
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

    def test_ambiguous_upload_retains_receipt_without_seal_or_retry(self):
        api, uploader, token = Mock(), Mock(side_effect=TimeoutError("sensitive URL")), Mock(return_value="sensitive JWT")
        inputs = {name: b"bytes" for name in client.BINARIES}
        value = receipt(uploads={name: {} for name in client.BINARIES})
        api.invoke.return_value = response(value)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            with self.assertRaises(TimeoutError):
                client.submit(api, "function", {}, inputs, path, audience="admission", bucket="bucket", region="region",
                              token_reader=token, uploader=uploader)
            self.assertTrue(path.exists())
            self.assertNotIn("sensitive", path.read_text())
        self.assertEqual(api.invoke.call_count, 1)
        self.assertEqual(uploader.call_count, 1)

    def test_rerun_receipt_does_not_upload_or_resubmit(self):
        api, uploader = Mock(), Mock()
        api.invoke.return_value = response(receipt())
        with tempfile.TemporaryDirectory() as directory:
            result = client.submit(api, "function", {}, {}, Path(directory) / "receipt.json",
                audience="admission", bucket="bucket", region="region", token_reader=lambda _: "token", uploader=uploader)
        self.assertEqual(result["run_key"], "repo-123-run-100")
        self.assertEqual(api.invoke.call_count, 1)
        uploader.assert_not_called()

    def test_uploads_all_six_then_seals_using_a_fresh_signed_token(self):
        api, uploader, token = Mock(), Mock(), Mock(side_effect=["begin-token", "seal-token"])
        api.invoke.side_effect = [response(receipt(uploads={name: {} for name in client.BINARIES})),
                                  response(receipt(execution_arn="exact", execution_status="submitted"))]
        with tempfile.TemporaryDirectory() as directory:
            result = client.submit(api, "function", {}, {name: b"x" for name in client.BINARIES},
                Path(directory) / "receipt.json", audience="admission", bucket="bucket", region="region",
                token_reader=token, uploader=uploader)
        self.assertEqual(result["execution_status"], "submitted")
        self.assertEqual(uploader.call_count, 6)
        self.assertEqual(token.call_count, 2)
        payloads = [json.loads(call.kwargs["Payload"]) for call in api.invoke.call_args_list]
        self.assertEqual([p["action"] for p in payloads], ["begin", "seal"])
        self.assertEqual([p["token"] for p in payloads], ["begin-token", "seal-token"])

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
