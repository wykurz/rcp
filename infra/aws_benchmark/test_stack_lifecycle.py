"""Mailbox data expires, but retained exclusive ownership cannot expire underneath it."""

from pathlib import Path
import unittest

import yaml


class MailboxLifecycleTests(unittest.TestCase):
    def test_owner_has_no_expiration_and_all_fixed_mailbox_payloads_remain_bounded(self):
        template = yaml.safe_load(Path(__file__).with_name("stack.yaml").read_text())
        rules = template["Resources"]["Artifacts"]["Properties"]["LifecycleConfiguration"]["Rules"]
        expirations = [rule for rule in rules if rule.get("Status") == "Enabled" and "ExpirationInDays" in rule]
        owner = "control/mailbox-owner.json"
        self.assertTrue(expirations)
        self.assertFalse(any(owner.startswith(rule.get("Prefix", "")) for rule in expirations))
        for key in ("input/baseline/rcp", "input/request.json", "output/results.zip", "output/status.json",
                    "output/durable-receipt.json", "control/window.json", "control/durable-request.json"):
            matching = [rule for rule in expirations if key.startswith(rule.get("Prefix", ""))]
            self.assertEqual(len(matching), 1, key)
            self.assertEqual(matching[0]["ExpirationInDays"], 7)
        self.assertTrue(any(rule.get("AbortIncompleteMultipartUpload", {}).get("DaysAfterInitiation") == 1
                            and not rule.get("Prefix") for rule in rules))
