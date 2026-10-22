"""Exercise the documented conditional retirement action entirely against a mock S3 client."""

import io
import json
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from infra.aws_benchmark import mailbox
from infra.aws_benchmark.json_bytes import encoded
from infra.aws_benchmark.test_safety import ClaimConflict


class RetirementExampleTests(unittest.TestCase):
    def exercise(self, temporary, *, observed_body=None, observed_etag='"reviewed-etag"', delete_error=None,
                 precondition_error=False, existing_intent=False):
        root = Path(temporary)
        owner = dict(schema_version=1, token="a" * 32, bucket="reviewed-bucket", protocol="operator-window",
                     run="123-1", request_sha256="b" * 64)
        body = encoded(owner)
        expected = root / "owner.json"
        intent = root / "retirement.json"
        expected.write_bytes(body)
        if existing_intent:
            intent.write_text("keep earlier evidence")
        observed_body = body if observed_body is None else observed_body
        s3 = Mock()
        s3.get_object.return_value = dict(ETag=observed_etag, ContentLength=len(observed_body),
                                         Body=io.BytesIO(observed_body))
        s3.delete_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 204}}
        s3.delete_object.side_effect = delete_error
        if precondition_error:
            s3.get_object.side_effect = ClaimConflict()
        doc = Path(__file__).with_name("MAILBOX_RETIREMENT.md").read_text()
        snippet = doc.split("```python\n", 1)[1].split("```", 1)[0]
        snippet = snippet.replace('"REVIEWED_BUCKET"', '"reviewed-bucket"').replace("REVIEWED_ETAG", "reviewed-etag")
        snippet = snippet.replace('"reviewed-owner.json"', repr(str(expected))).replace(
            '"owner-retirement-intent.json"', repr(str(intent)))
        self.s3, self.intent = s3, intent
        # execute the documented imports without requiring or initializing the optional SDK
        sdk, core, config = (ModuleType(name) for name in ("boto3", "botocore", "botocore.config"))
        sdk.client = Mock(return_value=s3)
        core.__path__ = []
        core.config = config
        config.Config = Mock(side_effect=lambda **settings: SimpleNamespace(**settings))
        with patch.dict("sys.modules", {"boto3": sdk, "botocore": core, "botocore.config": config}):
            exec(compile(snippet, "MAILBOX_RETIREMENT.md", "exec"), {})
        config.Config.assert_called_once_with(connect_timeout=5, read_timeout=15,
                                              retries={"total_max_attempts": 1})
        sdk.client.assert_called_once_with("s3", config=SimpleNamespace(connect_timeout=5, read_timeout=15,
                                            retries={"total_max_attempts": 1}))

    def test_reviewed_example_binds_body_and_etag_and_deletes_only_the_owner(self):
        with tempfile.TemporaryDirectory() as temporary:
            self.exercise(temporary)
            exact = dict(Bucket="reviewed-bucket", Key=mailbox.OWNER_KEY, IfMatch='"reviewed-etag"')
            self.s3.get_object.assert_called_once_with(**exact)
            self.s3.delete_object.assert_called_once_with(**exact)
            self.assertEqual(json.loads(self.intent.read_text())["owner"]["run"], "123-1")
            self.assertTrue(self.intent.with_suffix(".response.json").is_file())

    def test_changed_owner_or_existing_intent_blocks_deletion(self):
        for change, error in ((dict(observed_body=b"other-owner"), ValueError),
                              (dict(observed_etag='"new-owner"'), ValueError),
                              (dict(precondition_error=True), ClaimConflict),
                              (dict(existing_intent=True), FileExistsError)):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                with self.assertRaises(error):
                    self.exercise(temporary, **change)
                self.s3.delete_object.assert_not_called()
                self.assertFalse(self.intent.with_suffix(".response.json").exists())

    def test_uncertain_delete_is_not_retried_and_retains_unconfirmed_intent(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(TimeoutError):
                self.exercise(temporary, delete_error=TimeoutError("lost response"))
            self.s3.delete_object.assert_called_once()
            self.assertEqual(json.loads(self.intent.read_text())["outcome"], "not yet confirmed")
            self.assertFalse(self.intent.with_suffix(".response.json").exists())


if __name__ == "__main__":
    unittest.main()
