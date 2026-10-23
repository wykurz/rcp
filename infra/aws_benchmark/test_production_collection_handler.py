import os
import json
import unittest
from unittest.mock import Mock, patch

from . import production_collection_handler as handler
from .test_production_admission import Cloud


class CollectionHandlerTests(unittest.TestCase):
    def setUp(self):
        self.cloud = Cloud()
        self.api = dict(dynamodb=Mock())
        self.api["dynamodb"].get_item.return_value = {}
        self.service, self.collector, self.reader = Mock(), Mock(), Mock()
        self.collector.return_value.collect.return_value = dict(state="success", retired=True)

    def call(self):
        return handler.collect_active(self.api, self.cloud.config, self.cloud.machine, self.reader,
            service_factory=self.service, collector_factory=self.collector)

    def test_idle_does_not_read_reports_or_invoke_runtime(self):
        self.assertEqual(self.call(), dict(state="idle", retired=False))
        self.service.assert_not_called()
        self.collector.assert_not_called()

    def test_active_identity_is_taken_only_from_server_pointer(self):
        self.api["dynamodb"].get_item.return_value = dict(Item={
            "pk": {"S": "active"}, "run_key": {"S": "repo-123-run-100"}, "epoch": {"S": "a" * 64}})
        self.assertTrue(self.call()["retired"])
        self.assertEqual(self.service.call_args.args[2:], ("repo-123-run-100", "a" * 64))
        self.collector.return_value.collect.assert_called_once_with(retire=True)

    def test_malformed_or_denied_pointer_cannot_invoke_collector(self):
        for value in (dict(pk={"S": "active"}), dict(pk={"S": "active"}, run_key={"S": "../run"}, epoch={"S": "a" * 64}),
                      dict(pk={"S": "active"}, run_key={"S": "repo-123-run-100"}, epoch={"S": "bad"})):
            self.api["dynamodb"].get_item.return_value = dict(Item=value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.call()
        self.api["dynamodb"].get_item.side_effect = PermissionError("sensitive")
        with self.assertRaises(PermissionError):
            self.call()
        self.collector.assert_not_called()

    def test_handler_defaults_disabled_and_rejects_request_selected_run(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(handler, "collection_clients") as clients:
            self.assertEqual(handler.lambda_handler({}, None)["state"], "disabled")
            clients.assert_not_called()
        with patch.dict(os.environ, {"RCP_PRODUCTION_ENABLED": "true"}, clear=True), patch.object(handler, "collection_clients") as clients:
            self.assertEqual(handler.lambda_handler(dict(run_key="untrusted"), None)["state"], "held")
            clients.assert_not_called()

    def test_normal_held_result_emits_safe_diagnostic(self):
        env = dict(RCP_PRODUCTION_ENABLED="true", RCP_PRODUCTION_REPOSITORY="wykurz/rcp",
            RCP_PRODUCTION_REPOSITORY_ID="123", RCP_PRODUCTION_OWNER_ID="44",
            RCP_PRODUCTION_MACHINE_ARN=self.cloud.machine)
        with patch.dict(os.environ, env, clear=True), patch.object(handler.Configuration, "environment", return_value=self.cloud.config), \
             patch.object(handler, "collection_clients", return_value=self.api), \
             patch.object(handler, "collect_active", return_value=dict(state="held", retired=False, reason="proof unavailable")), \
             patch("builtins.print") as logged:
            self.assertEqual(handler.lambda_handler({}, None)["state"], "held")
        self.assertEqual(json.loads(logged.call_args.args[0]),
                         dict(event="production_collection_held", reason="proof unavailable"))

    def test_handler_error_response_does_not_expose_credentials(self):
        with patch.dict(os.environ, {"RCP_PRODUCTION_ENABLED": "true"}, clear=True), \
             patch.object(handler.Configuration, "environment", side_effect=RuntimeError("secret credential")), \
             patch("builtins.print") as logged:
            result = handler.lambda_handler({}, None)
        self.assertEqual(result["state"], "held")
        self.assertNotIn("secret", str(result))
        self.assertNotIn("secret", str(logged.call_args))
        self.assertEqual(json.loads(logged.call_args.args[0])["category"], "RuntimeError")


if __name__ == "__main__":
    unittest.main()
