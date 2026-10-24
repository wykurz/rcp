"""Exercise the real canary Git/HTTP adapters against local ruleset models."""

import contextlib
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from . import history_canary as canary
from .common import ValidationError
from .history_contract import HISTORY_REF
from .history_git import git_env

BINDING = canary.identity("0123456789abcdef", "a" * 64, "b" * 40)


class Server:
    def __init__(self, directory):
        self.path = Path(directory) / "server.git"
        subprocess.run(["git", "init", "--bare", str(self.path)], check=True, capture_output=True)
        hook = self.path / "hooks/pre-receive"
        hook.write_text("""#!/usr/bin/python3
import os, subprocess, sys
for line in sys.stdin:
    old,new,ref=line.strip().split()
    if ref == 'refs/heads/aws-benchmark-history':
        permitted = os.environ.get('CANARY_ACTOR') == 'writer' and new != '0'*40
        if permitted and old != '0'*40:
            permitted = subprocess.run(['git','merge-base','--is-ancestor',old,new]).returncode == 0
        if not permitted:
            print('error: GH013: Repository rule violations found for '+ref+'.',file=sys.stderr)
            sys.exit(1)
""")
        hook.chmod(0o700)
        self.wrappers = {}
        for actor in ("writer", "ordinary"):
            wrapper = Path(directory) / ("receive-" + actor)
            wrapper.write_text("#!/usr/bin/python3\nimport os,sys\nenv={k:v for k,v in os.environ.items() if not k.startswith('GIT_CONFIG')}\nenv['CANARY_ACTOR']='" + actor + "'\nos.execvpe('git-receive-pack',['git-receive-pack',*sys.argv[1:]],env)\n")
            wrapper.chmod(0o700)
            self.wrappers[actor] = wrapper
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                source = self.path.split("/branches/", 1)[1].removesuffix("/rename")
                import urllib.parse
                source = urllib.parse.unquote(source)
                destination = json.loads(self.rfile.read(int(self.headers["Content-Length"])))['new_name']
                if destination == HISTORY_REF.removeprefix("refs/heads/"):
                    status = 422
                    value = {"message": "Validation Failed", "errors": ["repository rules do not permit renaming branch '" + source + "' to '" + destination + "'"]}
                else:
                    before = owner.refs()
                    sha = before["refs/heads/" + source]
                    transaction = "start\ncreate refs/heads/" + destination + " " + sha + "\ndelete refs/heads/" + source + " " + sha + "\nprepare\ncommit\n"
                    subprocess.run(["git", "--git-dir=" + str(owner.path), "update-ref", "--stdin"], input=transaction, text=True, capture_output=True, check=True)
                    status, value = 201, {"name": destination}
                raw = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            def log_message(self, *_args):
                pass
        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.http.shutdown()
        self.thread.join()
        self.http.server_close()

    def refs(self):
        result = subprocess.run(["git", "--git-dir=" + str(self.path), "for-each-ref", "--format=%(objectname) %(refname)"], text=True, capture_output=True, check=True)
        return {line.split()[1]: line.split()[0] for line in result.stdout.splitlines()}

    def remote(self, path):
        remote = canary.Remote(path, "synthetic-ordinary", "synthetic-writer", sleep=lambda _seconds: None)
        original = remote.git
        def git(*args, token=None, input_data=None):
            args = list(args)
            if "push" in args:
                args.insert(args.index("push") + 1, "--receive-pack=" + str(self.wrappers[token]))
            return original(*args, token=token, input_data=input_data)
        remote.git = git
        return remote


@contextlib.contextmanager
def local_server():
    with tempfile.TemporaryDirectory() as directory:
        server = Server(directory)
        try:
            with patch.object(canary, "REMOTE", "file://" + str(server.path)), patch.object(canary, "API", "http://127.0.0.1:" + str(server.http.server_port)):
                yield Path(directory), server
        finally:
            server.close()


class RemoteCanaryTests(unittest.TestCase):
    def test_real_git_and_http_full_canary_enforces_writer_preservation(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            receipt = base / "receipt"
            run = canary.Canary(remote, receipt, BINDING)
            root = run.ordinary()
            run.writer(root)
            rows = [json.loads(line) for line in receipt.read_text().splitlines()]
            self.assertEqual(set(server.refs()), {HISTORY_REF})
            self.assertEqual(rows[-1]["stage"], "complete")
            rename = next(row["outcome"] for row in rows if row["stage"] == "ordinary_rename_history_outcome")
            self.assertEqual(rename["status"], 422)
            self.assertIn("repository rules do not permit renaming branch", rename["diagnostic"])
            for actor in ("writer", "ordinary"):
                row = next(row for row in rows if row["stage"] == actor + "_non_fast_forward_outcome")
                self.assertTrue(row["outcome"]["rule_denial"])
                self.assertIn("[remote rejected]", row["outcome"]["diagnostic"])
            self.assertNotIn("synthetic-", receipt.read_text())

    def test_read_failure_after_irreversible_fast_forward_resumes_without_repeating_mutations(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            receipt = base / "receipt"
            run = canary.Canary(remote, receipt, BINDING)
            root = run.ordinary()
            original_refs = remote.refs
            def fail_after_fast_forward():
                rows = [json.loads(line) for line in receipt.read_text().splitlines()]
                if rows[-1]["stage"] == "writer_fast_forward_outcome":
                    raise TimeoutError()
                return original_refs()
            with patch.object(remote, "refs", side_effect=fail_after_fast_forward):
                with self.assertRaises(TimeoutError):
                    run.writer(root)
            before = receipt.read_bytes()
            raw, plan, completed = canary.resume_state(receipt, BINDING, hashlib.sha256(before).hexdigest())
            self.assertEqual(completed, ("writer_create_history", "writer_fast_forward"))
            fresh = server.remote(base / "resume.git")
            self.assertEqual(canary.sentinel_plan(fresh, BINDING), plan)
            output = base / "resumed"
            output.write_bytes(raw)
            with patch.object(fresh, "push", wraps=fresh.push) as pushes:
                canary.Canary(fresh, output, BINDING).writer(plan["root"], completed)
            self.assertEqual(pushes.call_count, 6)
            self.assertEqual(json.loads(output.read_text().splitlines()[-1])["stage"], "complete")
            self.assertEqual(receipt.read_bytes(), before)
            self.assertEqual(set(server.refs()), {HISTORY_REF})

    def test_resume_rejects_changed_digest_plan_reordered_and_uncertain_evidence(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            receipt = base / "receipt"
            run = canary.Canary(remote, receipt, BINDING)
            root = run.ordinary()
            with patch.object(remote, "verify_history", side_effect=TimeoutError()):
                with self.assertRaises(TimeoutError):
                    run.writer(root)
            original = [json.loads(line) for line in receipt.read_text().splitlines()]
            with self.assertRaisesRegex(ValidationError, "digest differs"):
                canary.resume_state(receipt, BINDING, "0" * 64)
            def bad_plan(rows):
                next(row for row in rows if row["stage"] == "sentinels")["child"] = "e" * 40
            def reversed_outcome(rows):
                index = next(i for i, row in enumerate(rows) if row["stage"] == "writer_create_history_outcome")
                row = rows.pop(index)
                rows.insert(0, row)
            def uncertain(rows):
                rows.append(dict(BINDING, stage="writer_fast_forward_uncertain"))
            def foreign_observed(rows):
                next(row for row in rows if row["stage"] == "writer_fast_forward_observed")["refs"][HISTORY_REF] = "e" * 40
            def forged_ordinary(rows):
                next(row for row in rows if row["stage"] == "ordinary_create_history_outcome")["outcome"]["rule_denial"] = False
            for change in (bad_plan, reversed_outcome, uncertain, foreign_observed, forged_ordinary):
                with self.subTest(change=change.__name__):
                    rows = json.loads(json.dumps(original))
                    change(rows)
                    raw = b"".join(json.dumps(row).encode() + b"\n" for row in rows)
                    changed = base / "changed"
                    changed.write_bytes(raw)
                    with self.assertRaises(ValidationError):
                        canary.resume_state(changed, BINDING, hashlib.sha256(raw).hexdigest())

    def test_resume_after_negative_read_failure_preserves_proof_and_can_resume_again(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            receipt = base / "receipt"
            run = canary.Canary(remote, receipt, BINDING)
            root = run.ordinary()
            original_refs = remote.refs
            def fail_after_negative():
                rows = [json.loads(line) for line in receipt.read_text().splitlines()]
                if rows[-1]["stage"] == "writer_non_fast_forward_outcome":
                    raise TimeoutError()
                return original_refs()
            with patch.object(remote, "refs", side_effect=fail_after_negative):
                with self.assertRaises(TimeoutError):
                    run.writer(root)
            raw, plan, completed = canary.resume_state(receipt, BINDING, hashlib.sha256(receipt.read_bytes()).hexdigest())
            self.assertEqual(completed[-1], "writer_non_fast_forward")
            fresh = server.remote(base / "resume.git")
            self.assertEqual(canary.sentinel_plan(fresh, BINDING), plan)
            output = base / "resumed"
            output.write_bytes(raw)
            again = canary.Canary(fresh, output, BINDING)
            again.record("approved_resume", source_sha256=hashlib.sha256(raw).hexdigest(), completed=list(completed))
            with patch.object(fresh, "verify_history", side_effect=TimeoutError()):
                with self.assertRaises(TimeoutError):
                    again.writer(root, completed)
            _, _, next_completed = canary.resume_state(output, BINDING, hashlib.sha256(output.read_bytes()).hexdigest())
            self.assertEqual(next_completed, canary.WRITER_STAGES[:-1])
            final = server.remote(base / "final.git")
            canary.sentinel_plan(final, BINDING)
            canary.Canary(final, output, BINDING).writer(root, next_completed)
            self.assertEqual(set(server.refs()), {HISTORY_REF})

    def test_resume_rejects_stage_gap_outcome_before_ordinary_and_duplicate_uncertainty(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            receipt = base / "receipt"
            run = canary.Canary(remote, receipt, BINDING)
            root = run.ordinary()
            run.writer(root)
            original = [json.loads(line) for line in receipt.read_text().splitlines()]
            mutations = []
            gap = [row for row in original if not row["stage"].startswith("writer_fast_forward")]
            mutations.append(gap)
            ordered = json.loads(json.dumps(original))
            position = next(i for i, row in enumerate(ordered) if row["stage"] == "ordinary_complete")
            ordinary = ordered.pop(position)
            ordered.append(ordinary)
            mutations.append(ordered)
            mutations.append(original + [dict(BINDING, stage="unknown_write_uncertain")])
            mutations.append(original + [dict(BINDING, stage="unknown_baseline", refs={HISTORY_REF: "f" * 40})])
            mutations.append(original + [dict(BINDING, stage="writer_delete_outcome", outcome={"accepted": False, "rule_denial": True})])
            for rows in mutations:
                raw = b"".join(json.dumps(row).encode() + b"\n" for row in rows)
                changed = base / "changed"
                changed.write_bytes(raw)
                with self.assertRaises(ValidationError):
                    canary.resume_state(changed, BINDING, hashlib.sha256(raw).hexdigest())

    def test_retained_live_baseline_drift_requires_separate_reconciliation_even_if_restored(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            receipt = base / "receipt"
            run = canary.Canary(remote, receipt, BINDING)
            root = run.ordinary()
            original_refs = remote.refs
            def foreign_baseline():
                rows = [json.loads(line) for line in receipt.read_text().splitlines()]
                refs = original_refs()
                if rows[-1]["stage"] == "writer_create_history":
                    refs[HISTORY_REF] = "f" * 40
                return refs
            with patch.object(remote, "refs", side_effect=foreign_baseline):
                with self.assertRaisesRegex(ValidationError, "baseline changed"):
                    run.writer(root)
            self.assertEqual(server.refs()[HISTORY_REF], root)
            with self.assertRaisesRegex(ValidationError, "retained baseline"):
                canary.resume_state(receipt, BINDING, hashlib.sha256(receipt.read_bytes()).hexdigest())

    def test_real_fetch_requires_expected_tip_and_zero_records(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            root = remote.commit()
            self.assertTrue(remote.push("writer", HISTORY_REF, root, expected="")["accepted"])
            with self.assertRaisesRegex(ValidationError, "publisher-compatible"):
                remote.verify_history("a" * 40)
            with patch.object(canary, "history", return_value=({"unapproved": {}}, root)):
                with self.assertRaisesRegex(ValidationError, "publisher-compatible"):
                    remote.verify_history(root)

    def test_real_resume_check_cli_validates_plan_without_credentials_or_mutations(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            receipt = base / "receipt"
            run = canary.Canary(remote, receipt, BINDING)
            root = run.ordinary()
            with patch.object(remote, "verify_history", side_effect=TimeoutError()):
                with self.assertRaises(TimeoutError):
                    run.writer(root)
            before = server.refs()
            command = [sys.executable, "-m", "infra.aws_benchmark.history_canary", "resume-check", "--probe", BINDING["probe"],
                "--trust-sha256", BINDING["trust_sha256"], "--revision", BINDING["revision"], "--directory", str(base / "new-directory"),
                "--evidence", str(base / "new-evidence"), "--source-evidence", str(receipt), "--source-sha256", hashlib.sha256(receipt.read_bytes()).hexdigest()]
            env = {key: value for key, value in os.environ.items() if key not in ("GH_TOKEN", "GITHUB_TOKEN", "WRITER_TOKEN")}
            result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((base / "new-directory").exists())
            self.assertFalse((base / "new-evidence").exists())
            self.assertEqual(server.refs(), before)
            command[-1] = "0" * 64
            result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("digest differs", result.stderr)

    def test_failed_scoped_read_and_existing_squat_leave_evidence_without_mutation(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            receipt = base / "receipt"
            with patch.object(canary, "REMOTE", "file://" + str(base / "missing")):
                with self.assertRaisesRegex(ValidationError, "read failed"):
                    canary.Canary(remote, receipt, BINDING).ordinary()
            self.assertEqual(json.loads(receipt.read_text())["stage"], "initial_observation_read_failed")
            self.assertEqual(server.refs(), {})
            root = remote.commit()
            for ref in (HISTORY_REF + "/squat", "refs/heads/noise"):
                self.assertTrue(remote.push("ordinary", ref, root, expected="")["accepted"])
            receipt = base / "squat-receipt"
            with self.assertRaisesRegex(ValidationError, "untrusted bootstrap"):
                canary.Canary(remote, receipt, BINDING).ordinary()
            row = json.loads(receipt.read_text())
            self.assertEqual(row["refs"], {HISTORY_REF + "/squat": root})

    def test_scoped_ref_read_ignores_large_unrelated_namespace(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            root = remote.commit()
            self.assertTrue(remote.push("ordinary", "refs/heads/control", root, expected="")["accepted"])
            updates = "".join("create refs/heads/unrelated/" + str(index) + "x" * 100 + " " + root + "\n" for index in range(800))
            subprocess.run(["git", "--git-dir=" + str(server.path), "update-ref", "--stdin"], input=updates, text=True, capture_output=True, check=True)
            remote.probe = BINDING["probe"]
            self.assertEqual(remote.refs(), {})

    def test_real_lease_rejects_replacing_unexpected_control_tip(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            first = remote.commit(label="first")
            foreign = remote.commit(first, label="foreign")
            wanted = remote.commit(first, label="wanted")
            ref = canary.control_refs(BINDING["probe"])[0]
            self.assertTrue(remote.push("ordinary", ref, foreign, expected="")["accepted"])
            result = remote.push("ordinary", ref, wanted, expected=first)
            self.assertFalse(result["accepted"])
            self.assertFalse(result["rule_denial"])
            self.assertEqual(server.refs()[ref], foreign)

    def test_rename_classifier_requires_exact_names_not_generic_http_errors(self):
        source, target = "refs/heads/control", HISTORY_REF
        text = "repository rules do not permit renaming branch 'control' to 'aws-benchmark-history'"
        for value in ({"message": "Validation Failed", "errors": [text]}, {"message": text},
                      {"message": "Validation Failed", "errors": [{"message": text}]}):
            self.assertTrue(canary.rename_denial(422, value, source, target)[0])
        for value in ({"message": "Validation Failed"}, {"message": "Forbidden"},
                      {"message": "Validation Failed", "errors": [text.replace("control", "other")]}):
            self.assertFalse(canary.rename_denial(422, value, source, target)[0])

    def test_observation_and_fetch_use_only_existing_ordinary_auth_header(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            remote.probe = BINDING["probe"]
            completed = subprocess.CompletedProcess([], 0, "", "")
            with patch.object(canary.subprocess, "run", return_value=completed) as call:
                self.assertEqual(remote.refs(), {})
                with patch.object(canary, "history", return_value=({}, "a" * 40)):
                    remote.verify_history("a" * 40)
            self.assertEqual(call.call_count, 2)
            expected = "AUTHORIZATION: basic " + base64.b64encode(b"x-access-token:synthetic-ordinary").decode()
            for invocation in call.call_args_list:
                env = invocation.kwargs["env"]
                self.assertEqual(env["GIT_CONFIG_KEY_2"], "http.https://github.com/.extraheader")
                self.assertEqual(env["GIT_CONFIG_VALUE_2"], expected)
                self.assertNotIn("synthetic-writer", str(env))
                self.assertNotIn("GH_TOKEN", env)
                self.assertNotIn("WRITER_TOKEN", env)
                self.assertNotIn("synthetic-ordinary", str(invocation.args))

    def test_auth_denial_is_not_retried_and_receipt_does_not_echo_token(self):
        for message in ("fatal: Authentication failed", "fatal: The requested URL returned error: 403",
                        "fatal: SSL certificate problem", "fatal: repository not found", "unknown Git failure"):
            with self.subTest(message=message), local_server() as (base, server):
                remote = server.remote(base / "client.git")
                receipt = base / "receipt"
                bad = subprocess.CompletedProcess([], 128, "", message + " synthetic-ordinary")
                with patch.object(remote, "git", return_value=bad) as call:
                    with self.assertRaisesRegex(ValidationError, "no retry"):
                        canary.Canary(remote, receipt, BINDING).ordinary()
                self.assertEqual(call.call_count, 1)
                row = json.loads(receipt.read_text())
                self.assertIn("access denied", row["reason"])
                self.assertNotIn("synthetic-ordinary", receipt.read_text())
                self.assertNotIn(message, receipt.read_text())

    def test_authenticated_response_bound_and_public_diagnostic_redaction(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            remote.probe = BINDING["probe"]
            result = subprocess.CompletedProcess([], 0, "x" * (canary.LIMIT + 1), "")
            with patch.object(canary.subprocess, "run", return_value=result) as call:
                with self.assertRaisesRegex(ValidationError, "response exceeds bound"):
                    remote.refs()
            self.assertEqual(call.call_count, 1)
            basic = base64.b64encode(b"x-access-token:synthetic-ordinary").decode()
            self.assertEqual(remote.public_text("Bearer synthetic-ordinary Basic " + basic), "Bearer [redacted] Basic [redacted]")
            self.assertEqual(canary.read_reason(ValidationError("synthetic-ordinary")), "read failed")

    def test_idempotent_reads_retry_but_mutation_is_single_shot(self):
        with local_server() as (base, server):
            remote = server.remote(base / "client.git")
            remote.probe = BINDING["probe"]
            good = subprocess.CompletedProcess([], 0, "", "")
            bad = subprocess.CompletedProcess([], 1, "", "fatal: unable to access: The requested URL returned error: 503")
            with patch.object(remote, "git", side_effect=[bad, subprocess.TimeoutExpired("git", 1), good]) as calls:
                self.assertEqual(remote.refs(), {})
                self.assertEqual(calls.call_count, 3)
            with patch.object(remote, "git", side_effect=TimeoutError()) as calls:
                with self.assertRaises(TimeoutError):
                    remote.push("writer", HISTORY_REF, "a" * 40, expected="")
                self.assertEqual(calls.call_count, 1)


if __name__ == "__main__":
    unittest.main()
