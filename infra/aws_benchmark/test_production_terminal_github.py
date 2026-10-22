import copy
import json
import unittest
from unittest.mock import Mock

from .production_github import TerminalProducerReader, GitHubRunError, _TERMINAL_PATH
from .production_policy import producer_identity
from .test_production_policy import identity, run_json


class TerminalProducerReaderTests(unittest.TestCase):
    def setUp(self):
        self.identity = identity()
        self.producer = producer_identity(self.identity)
        self.run = run_json(self.identity) | dict(status="completed", conclusion="success")
        self.job = dict(id=7, run_id=int(self.identity.run_id), run_attempt=1, head_sha=self.identity.sha,
                        status="completed", conclusion="success", completed_at="2026-10-22T17:00:00Z")
        self.jobs = dict(total_count=1, jobs=[self.job])
        self.path = "/repos/" + self.identity.repository + "/actions/runs/" + self.identity.run_id
        self.values = {self.path: self.run, self.path + "/attempts/1": self.run,
                       self.path + "/attempts/1/jobs?per_page=100": self.jobs,
                       self.path + "/pending_deployments": []}
        self.fetch = Mock(side_effect=lambda path: json.dumps(self.values[path]).encode())
        self.reader = TerminalProducerReader(repository=self.identity.repository,
            repository_id=self.identity.repository_id, repository_owner_id=self.identity.repository_owner_id,
            fetcher=self.fetch)

    def test_reads_original_jobs_and_rechecks_current_attempt_without_credentials(self):
        value = self.reader.read(self.producer)
        self.assertEqual(value["attempt_jobs"], self.jobs)
        self.assertEqual(value["current_jobs"], self.jobs)
        self.assertEqual(value["pending_deployments"], [])
        paths = [call.args[0] for call in self.fetch.call_args_list]
        self.assertEqual(paths[0], self.path + "/attempts/1")
        self.assertEqual(paths[-1], self.path)
        self.assertTrue(all(_TERMINAL_PATH.fullmatch(path) for path in paths))

    def test_newer_attempt_requires_its_complete_job_inventory(self):
        self.values[self.path] = self.run | dict(run_attempt=2)
        newer_jobs = dict(total_count=1, jobs=[self.job | dict(id=8, run_attempt=2)])
        self.values[self.path + "/attempts/2/jobs?per_page=100"] = newer_jobs
        value = self.reader.read(self.producer)
        self.assertEqual(value["current_jobs"], newer_jobs)
        self.assertEqual(value["attempt_jobs"], self.jobs)

    def test_empty_newer_cancelled_or_startup_failed_rerun_preserves_original_jobs(self):
        self.values[self.path + "/attempts/2/jobs?per_page=100"] = dict(total_count=0, jobs=[])
        for conclusion in ("cancelled", "startup_failure"):
            self.values[self.path] = self.run | dict(run_attempt=2, conclusion=conclusion)
            value = self.reader.read(self.producer)
            self.assertEqual(value["current_jobs"], dict(total_count=0, jobs=[]))
            self.assertEqual(value["attempt_jobs"], self.jobs)
        for status, conclusion in (("completed", "success"), ("completed", "failure"), ("in_progress", None)):
            self.values[self.path] = self.run | dict(run_attempt=2, status=status, conclusion=conclusion)
            with self.subTest(status=status, conclusion=conclusion), self.assertRaises(GitHubRunError):
                self.reader.read(self.producer)

    def test_rerun_during_reads_holds_instead_of_returning_partial_proof(self):
        count = 0
        def fetch(path):
            nonlocal count
            if path == self.path:
                count += 1
                if count == 2:
                    return json.dumps(self.run | dict(run_attempt=2, status="in_progress", conclusion=None)).encode()
            return json.dumps(self.values[path]).encode()
        self.fetch.side_effect = fetch
        with self.assertRaises(GitHubRunError):
            self.reader.read(self.producer)

    def test_truncated_oversized_and_empty_job_inventories_are_rejected(self):
        key = self.path + "/attempts/1/jobs?per_page=100"
        for value in (dict(total_count=2, jobs=[self.job]), dict(total_count=101, jobs=[self.job] * 101),
                      dict(total_count=0, jobs=[]), dict(total_count=True, jobs=[self.job])):
            self.values[key] = value
            with self.subTest(value=value["total_count"]), self.assertRaises(GitHubRunError):
                self.reader.read(self.producer)

    def test_wrong_repository_and_untrusted_paths_are_rejected_before_reads(self):
        for change in (dict(repository="other/repo"), dict(repository_id="999"), dict(run_id="../100")):
            with self.subTest(change=change), self.assertRaises(GitHubRunError):
                self.reader.read(self.producer | change)
        self.fetch.assert_not_called()
        for path in ("https://evil.example", self.path + "/jobs?per_page=100", self.path + "/attempts/1/jobs?page=2",
                     self.path + "/../secrets", self.path + "/pending_deployments?token=x"):
            self.assertIsNone(_TERMINAL_PATH.fullmatch(path))

    def test_access_failure_is_sanitized_and_never_proof_of_no_pending_jobs(self):
        self.fetch.side_effect = RuntimeError("secret endpoint body")
        with self.assertRaisesRegex(GitHubRunError, "terminal producer") as caught:
            self.reader.read(self.producer)
        self.assertNotIn("secret", str(caught.exception))
        self.assertEqual(self.fetch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
