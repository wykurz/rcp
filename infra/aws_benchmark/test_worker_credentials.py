"""The worker's AWS credentials outlast a long run only if their renewal is retried; no AWS calls."""

import os
import sys
import unittest
from unittest.mock import patch

from . import worker

SETTING = "AWS_METADATA_SERVICE_NUM_ATTEMPTS"


def without(*names):
    """This process's environment, minus what would decide the outcome of a test."""
    return {key: value for key, value in os.environ.items() if key not in names}


class CredentialRenewalTests(unittest.TestCase):
    def started(self, environment):
        """Run the worker's entry point up to its first client; return the setting that client saw."""
        seen = []
        def clients(region):
            seen.append(os.environ.get(SETTING))
            return {"s3": object()}
        with patch.dict(os.environ, environment, clear=True), patch.object(worker, "clients", side_effect=clients), \
             patch.object(worker, "run") as run, patch.object(worker.os, "geteuid", return_value=0), \
             patch.object(worker.os, "chdir"), \
             patch.object(sys, "argv", ["worker", "--region", "us-east-1", "--bucket", "bucket"]):
            worker.main()
        run.assert_called_once()
        return seen

    def test_the_sdk_is_told_to_ask_the_metadata_service_again_before_any_client_exists(self):
        # the role's credentials last about six hours: a longer run renews them at its final upload,
        # and one failed read of the metadata service must not cost that run its results
        self.assertEqual(worker.METADATA_ATTEMPTS, 5)
        self.assertEqual(self.started(without(SETTING)), ["5"])

    def test_a_setting_that_is_already_there_is_kept(self):
        self.assertEqual(self.started(dict(without(SETTING), **{SETTING: "9"})), ["9"])

    def test_the_sdk_reads_that_setting_and_asks_once_without_it(self):
        import botocore.session
        quiet = dict(without(SETTING), AWS_CONFIG_FILE=os.devnull, AWS_SHARED_CREDENTIALS_FILE=os.devnull)
        with patch.dict(os.environ, quiet, clear=True):
            self.assertEqual(botocore.session.get_session().get_config_variable("metadata_service_num_attempts"), 1)
        with patch.dict(os.environ, dict(quiet, **{SETTING: str(worker.METADATA_ATTEMPTS)}), clear=True):
            self.assertEqual(botocore.session.get_session().get_config_variable("metadata_service_num_attempts"),
                             worker.METADATA_ATTEMPTS)


if __name__ == "__main__":
    unittest.main()
