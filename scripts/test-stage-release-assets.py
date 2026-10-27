#!/usr/bin/env python3
import copy
import hashlib
import http.client
import importlib.util
import io
import json
import os
from pathlib import Path
import runpy
import stat
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
import urllib.error
import zipfile
import zlib


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/stage-release-assets.py"
spec = importlib.util.spec_from_file_location("stage_release_assets", SCRIPT)
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)


def archive(entries, compression=zipfile.ZIP_STORED):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=compression) as output:
        for name, data in entries:
            output.writestr(name, data)
    return stream.getvalue()


class Response(io.BytesIO):
    status = 200


class Transport:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def open(self, request, timeout):
        self.calls.append((request, timeout))
        value = self.responses[request.full_url]
        if isinstance(value, Exception):
            raise value
        return Response(value)


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {"GITHUB_REPOSITORY": "wykurz/rcp", "GITHUB_REPOSITORY_ID": "655073755",
                    "GITHUB_RUN_ID": "12345", "GITHUB_RUN_ATTEMPT": "2", "GITHUB_SHA": "a" * 40,
                    "RELEASE_COMMIT": "b" * 40, "RELEASE_TAG": "v1.2.3-rc.1+build.7",
                    "RUNNER_TEMP": str(self.root), "GITHUB_OUTPUT": str(self.root / "output"),
                    "GH_TOKEN": "test-only-not-a-real-token"}
        self.responses, self.metadata, self.payloads = {}, {}, {}
        for artifact_id, kind in enumerate(M.KINDS, 1):
            self.env[kind.replace("-", "_").upper() + "_ID"] = str(artifact_id)
            # even hostile-looking inner paths stay opaque; no package extraction or execution
            payload = archive([("../../never-execute.sh", b"exit 71")])
            blob = archive([(self.filename(kind), payload)])
            endpoint = self.endpoint(artifact_id)
            self.metadata[artifact_id] = {
                "id": artifact_id, "name": "release-" + kind + "-" + self.env["RELEASE_COMMIT"] + "-1",
                "size_in_bytes": len(blob), "expired": False, "digest": "sha256:" + hashlib.sha256(blob).hexdigest(),
                "workflow_run": {"id": 12345, "repository_id": 655073755, "head_repository_id": 655073755,
                                 "head_sha": self.env["GITHUB_SHA"]}}
            self.payloads[kind] = payload
            self.responses[endpoint + "/zip"] = blob
        self.sync_metadata()

    def endpoint(self, artifact_id):
        return M.API + "/repos/wykurz/rcp/actions/artifacts/" + str(artifact_id)

    def filename(self, kind):
        return "rcp-" + kind + "-" + self.env["RELEASE_TAG"] + ".zip"

    def sync_metadata(self):
        for artifact_id, data in self.metadata.items():
            self.responses[self.endpoint(artifact_id)] = json.dumps(data).encode()

    def client(self):
        self.transport = Transport(self.responses)
        return M.Client(self.env["GH_TOKEN"], self.transport)

    def replace_blob(self, blob):
        self.responses[self.endpoint(1) + "/zip"] = blob
        self.metadata[1].update(size_in_bytes=len(blob), digest="sha256:" + hashlib.sha256(blob).hexdigest())
        self.sync_metadata()

    def rejected(self, env=None, client=None):
        with self.assertRaises((M.Rejected, zipfile.BadZipFile, ValueError, OSError)):
            M.stage(env or self.env, client or self.client())
        self.assertEqual(list(self.root.glob("release-assets-*")), [])
        self.assertFalse((self.root / "output").exists())

    def test_stages_exact_four_opaque_files_from_earlier_attempt_without_execution(self):
        result = M.stage(self.env, self.client())
        self.assertEqual({p.name for p in result.iterdir()}, {self.filename(k) for k in M.KINDS})
        for kind in M.KINDS:
            self.assertEqual((result / self.filename(kind)).read_bytes(), self.payloads[kind])
        self.assertFalse((self.root / "never-execute.sh").exists())
        self.assertTrue(all(0 < timeout <= 30 for _, timeout in self.transport.calls))

    def test_cli_emits_one_directory_only_after_complete_success(self):
        transport = Transport(self.responses)
        with mock.patch.dict(os.environ, self.env, clear=True), mock.patch("urllib.request.build_opener", return_value=transport):
            runpy.run_path(str(SCRIPT), run_name="__main__")
        output = (self.root / "output").read_text()
        self.assertEqual(len(output.splitlines()), 1)
        directory = Path(output.removeprefix("directory=").strip())
        self.assertEqual({p.name for p in directory.iterdir()}, {self.filename(k) for k in M.KINDS})

    def test_cli_partial_failure_emits_no_output_or_signed_url_or_token(self):
        self.responses[self.endpoint(4)] = urllib.error.URLError("signed-url?sig=secret " + self.env["GH_TOKEN"])
        stderr = io.StringIO()
        with mock.patch.dict(os.environ, self.env, clear=True), mock.patch("urllib.request.build_opener", return_value=Transport(self.responses)), mock.patch("sys.stderr", stderr):
            with self.assertRaises(SystemExit) as result:
                runpy.run_path(str(SCRIPT), run_name="__main__")
        self.assertEqual(result.exception.code, 1)
        self.assertNotIn("secret", stderr.getvalue())
        self.assertNotIn(self.env["GH_TOKEN"], stderr.getvalue())
        self.assertFalse((self.root / "output").exists())
        self.assertEqual(list(self.root.glob("release-assets-*")), [])

    def cli_failure(self, reason, env=None):
        stderr, transport = io.StringIO(), Transport(self.responses)
        with mock.patch.dict(os.environ, env or self.env, clear=True), mock.patch("urllib.request.build_opener", return_value=transport), mock.patch("sys.stderr", stderr):
            with self.assertRaises(SystemExit) as result:
                runpy.run_path(str(SCRIPT), run_name="__main__")
        self.assertEqual(result.exception.code, 1)
        self.assertIn(reason, stderr.getvalue())
        self.assertIn("no release upload authorized", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertNotIn("signed-secret", stderr.getvalue())
        self.assertNotIn(self.env["GH_TOKEN"], stderr.getvalue())
        self.assertFalse((self.root / "output").exists())
        self.assertEqual(list(self.root.glob("release-assets-*")), [])
        return transport

    def test_cli_preserves_authored_rejection_reasons_without_external_details(self):
        original = copy.deepcopy(self.metadata[4])
        for key, value, reason in (("expired", True, "artifact expired or unavailable"),
                                   ("id", 99, "artifact ID mismatch"),
                                   ("digest", "sha256:" + "0" * 64, "artifact archive size or SHA256 mismatch")):
            with self.subTest(reason=reason):
                self.metadata[4] = dict(original, **{key: value})
                self.sync_metadata()
                self.cli_failure(reason)
        self.metadata[4] = original
        self.sync_metadata()
        self.responses[self.endpoint(4)] = urllib.error.HTTPError("https://storage.example/?signed-secret", 410, self.env["GH_TOKEN"], {}, None)
        self.cli_failure("artifact HTTP request failed: 410")

    def test_cli_redacts_unexpected_transport_decoder_and_environment_exceptions(self):
        secret = "https://storage.example/?signed-secret=" + self.env["GH_TOKEN"]
        errors = [zlib.error(secret), EOFError(secret), http.client.IncompleteRead(secret.encode(), 1000),
                  http.client.HTTPException(secret), KeyError(secret), RuntimeError(secret)]
        for error in errors:
            with self.subTest(kind=type(error).__name__):
                self.responses[self.endpoint(4)] = error
                self.cli_failure("unexpected staging error")

    def test_cli_redacts_actual_corrupt_deflate_member_failure(self):
        filename = self.filename(M.KINDS[0])
        blob = bytearray(archive([(filename, self.payloads[M.KINDS[0]])], zipfile.ZIP_DEFLATED))
        name_size, extra_size = struct.unpack_from("<HH", blob, 26)
        blob[30 + name_size + extra_size] = 7  # reserved deflate block type
        with zipfile.ZipFile(io.BytesIO(blob)) as bundle, self.assertRaises(zlib.error):
            bundle.read(filename)
        self.replace_blob(bytes(blob))  # valid outer digest, failure in inner decompression
        self.cli_failure("unexpected staging error")

    def test_cli_validates_runner_paths_before_network_and_cleans_output_failure(self):
        for key, reason in (("RUNNER_TEMP", "missing runner temporary directory"),
                            ("GITHUB_OUTPUT", "missing workflow output path")):
            with self.subTest(key=key):
                env = dict(self.env)
                del env[key]
                transport = self.cli_failure(reason, env)
                self.assertEqual(transport.calls, [])
        self.cli_failure("unexpected staging error", dict(self.env, GITHUB_OUTPUT=str(self.root)))

    def test_cli_preserves_interruption_and_cleans_partial_staging(self):
        transport = Transport(self.responses)
        original_open = transport.open
        def interrupted(request, timeout):
            if request.full_url == self.endpoint(4):
                raise KeyboardInterrupt()
            return original_open(request, timeout)
        transport.open = interrupted
        with mock.patch.dict(os.environ, self.env, clear=True), mock.patch("urllib.request.build_opener", return_value=transport):
            with self.assertRaises(KeyboardInterrupt):
                runpy.run_path(str(SCRIPT), run_name="__main__")
        self.assertFalse((self.root / "output").exists())
        self.assertEqual(list(self.root.glob("release-assets-*")), [])

    def test_inner_asset_copy_does_not_hash_but_keeps_size_crc_and_type_validation(self):
        filename = self.filename(M.KINDS[0])
        blob = self.responses[self.endpoint(1) + "/zip"]
        with mock.patch.object(M.hashlib, "sha256", side_effect=AssertionError("unneeded inner digest")):
            destination = self.root / filename
            M.unpack(self.client(), io.BytesIO(blob), destination, filename)
        self.assertEqual(destination.read_bytes(), self.payloads[M.KINDS[0]])
        corrupt = bytearray(blob)
        name_size, extra_size = struct.unpack_from("<HH", corrupt, 26)
        corrupt[30 + name_size + extra_size] ^= 1
        with self.assertRaises(zipfile.BadZipFile):
            M.unpack(self.client(), io.BytesIO(corrupt), self.root / "corrupt.zip", filename)

    def test_rejects_foreign_missing_expired_future_or_unbound_metadata(self):
        original = copy.deepcopy(self.metadata[1])
        changes = [("id", 2), ("id", True), ("expired", True), ("expired", None), ("name", "other"),
                   ("name", original["name"][:-1] + "3"), ("name", original["name"].replace("b" * 40, "c" * 40)),
                   ("digest", None), ("digest", "sha256:" + "z" * 64), ("size_in_bytes", 0),
                   ("size_in_bytes", True), ("size_in_bytes", M.MAX_ARCHIVE + 1), ("workflow_run", None)]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                self.metadata[1] = dict(original, **{key: value})
                self.sync_metadata()
                self.rejected()
        for key, value in (("id", 2), ("repository_id", 2), ("head_repository_id", 2), ("head_sha", "c" * 40)):
            with self.subTest(run_key=key):
                self.metadata[1] = copy.deepcopy(original)
                self.metadata[1]["workflow_run"][key] = value
                self.sync_metadata()
                self.rejected()

    def test_rejects_invalid_or_duplicate_input_ids_before_network(self):
        for key, value in (("DEB_AMD64_ID", ""), ("DEB_AMD64_ID", "../1"), ("DEB_AMD64_ID", "2"),
                           ("GITHUB_REPOSITORY", "host/repo/../../other"), ("RELEASE_TAG", "v1\nanything"),
                           ("GITHUB_SHA", "refs/heads/main"), ("RELEASE_COMMIT", "../bad")):
            with self.subTest(key=key):
                client = self.client()
                self.rejected(dict(self.env, **{key: value}), client)
                self.assertEqual(self.transport.calls, [])

    def test_download_redirect_strips_auth_and_rejects_redirect_chains(self):
        endpoint = self.endpoint(1) + "/zip"
        signed = "https://storage.example/asset?sig=sensitive"
        blob = self.responses[endpoint]
        self.responses[endpoint] = urllib.error.HTTPError(endpoint, 302, "Found", {"Location": signed}, None)
        self.responses[signed] = blob
        M.stage(self.env, self.client())
        request = next(req for req, _ in self.transport.calls if req.full_url == signed)
        self.assertNotIn("Authorization", request.headers)
        self.assertTrue(all(req.headers.get("Authorization") == "Bearer " + self.env["GH_TOKEN"]
                            for req, _ in self.transport.calls if req.full_url.startswith(M.API + "/")))
        self.responses[signed] = urllib.error.HTTPError(signed, 302, "Found", {"Location": "https://elsewhere/"}, None)
        with self.assertRaises(M.Rejected):
            self.client().open(endpoint, authenticated=True, redirect=True)

    def test_rejects_auth_redirects_from_metadata_and_unsafe_download_locations(self):
        endpoint = self.endpoint(1)
        self.responses[endpoint] = urllib.error.HTTPError(endpoint, 302, "Found", {"Location": "https://elsewhere/"}, None)
        self.rejected()
        self.sync_metadata()
        for target in ("http://storage.example/a", "file:///etc/passwd", "https://user:pass@storage.example/a", "https://storage.example:444/a", "https://storage.example/a#fragment"):
            with self.subTest(target=target):
                self.responses[endpoint + "/zip"] = urllib.error.HTTPError(endpoint, 302, "Found", {"Location": target}, None)
                self.rejected()
        self.assertIsNone(M.NoRedirect().redirect_request(None, None, 302, "Found", {}, "https://elsewhere/"))

    def test_rejects_truncation_extra_bytes_and_digest_mismatch(self):
        endpoint = self.endpoint(1) + "/zip"
        original = self.responses[endpoint]
        for blob in (original[:-1], original + b"x", original[:-1] + b"x"):
            with self.subTest(length=len(blob)):
                self.responses[endpoint] = blob
                self.rejected()

    def test_rejects_outer_traversal_absolute_symlink_directory_duplicates_and_extras(self):
        filename, payload = self.filename(M.KINDS[0]), self.payloads[M.KINDS[0]]
        symlink = zipfile.ZipInfo(filename)
        symlink.create_system = 3
        symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
        cases = [[("../" + filename, payload)], [("/" + filename, payload)], [(filename + "/", payload)],
                 [(symlink, b"/etc/passwd")], [(filename, payload), ("extra", b"x")],
                 [(filename, payload), (filename, payload)]]
        for entries in cases:
            with self.subTest(entries=str(entries)[:100]):
                self.replace_blob(archive(entries))
                self.rejected()

    def test_rejects_invalid_zip_or_nonzip_asset(self):
        for blob in (b"not a zip", archive([(self.filename(M.KINDS[0]), b"not an inner zip")])):
            self.replace_blob(blob)
            self.rejected()

    def test_stream_metadata_and_uncompressed_limits_are_independent(self):
        client = self.client()
        with self.assertRaises(M.Rejected):
            client.copy(io.BytesIO(b"12345"), io.BytesIO(), 4)
        self.responses[self.endpoint(1)] = b" " * (M.MAX_METADATA + 1)
        self.rejected()
        self.sync_metadata()
        self.replace_blob(archive([(self.filename(M.KINDS[0]), b"x" * 10000)], zipfile.ZIP_DEFLATED))
        with mock.patch.object(M, "MAX_ASSET", 500):
            self.rejected()

    def test_deadline_and_late_failure_remove_partial_staging(self):
        client = self.client()
        client.deadline = time.monotonic() - 1
        self.rejected(client=client)
        self.metadata[4]["expired"] = True
        self.sync_metadata()
        self.rejected()

    def test_actual_isolated_cli_ignores_workspace_and_pythonpath_imports(self):
        hostile = self.root / "hostile"
        hostile.mkdir()
        marker = self.root / "executed"
        (hostile / "json.py").write_text("from pathlib import Path; Path(" + repr(str(marker)) + ").touch()")
        env = dict(os.environ, **self.env, PYTHONPATH=str(hostile))
        env["GH_TOKEN"] = ""
        result = subprocess.run([sys.executable, "-I", str(SCRIPT)], cwd=hostile, env=env,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(result.returncode, 1)
        self.assertIn("no release upload authorized", result.stderr)
        self.assertFalse(marker.exists())
        self.assertFalse((self.root / "output").exists())

    def test_expired_artifact_http_denial_holds_upload(self):
        self.responses[self.endpoint(1) + "/zip"] = urllib.error.HTTPError(self.endpoint(1), 410, "Gone", {}, None)
        self.rejected()

    def test_no_off_origin_authenticated_request(self):
        client = self.client()
        with self.assertRaises(M.Rejected):
            client.open("https://api.github.com.attacker.example/repos/a/b", authenticated=True)
        self.assertEqual(self.transport.calls, [])


if __name__ == "__main__":
    unittest.main()
