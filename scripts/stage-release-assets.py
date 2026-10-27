#!/usr/bin/env python3
"""Stage four immutable Actions artifacts as opaque, bounded release assets."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile


KINDS = ("deb-amd64", "rpm-amd64", "deb-arm64", "rpm-arm64")
MAX_ASSET = 512 * 1024 * 1024
MAX_ARCHIVE = MAX_ASSET + 1024 * 1024
MAX_METADATA = 64 * 1024
API = "https://api.github.com"


class Rejected(ValueError):
    """A fixed, credential-free diagnostic authored by this module."""


def require(condition, message):
    if not condition:
        raise Rejected(message)


def number(value):
    require(isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,19}", value), "invalid numeric identity")
    return int(value)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self, token, opener=None):
        self.token = token
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.deadline = time.monotonic() + 600

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, "artifact staging deadline exceeded")
        return min(30, remaining)

    def open(self, url, authenticated=False, redirect=False):
        headers = {"User-Agent": "rcp-release-assets", "Accept": "application/vnd.github+json"}
        if authenticated:
            require(url.startswith(API + "/repos/"), "refusing off-origin authentication")
            headers.update(Authorization="Bearer " + self.token, **{"X-GitHub-Api-Version": "2026-03-10"})
        try:
            response = self.opener.open(urllib.request.Request(url, headers=headers), timeout=self.remaining())
        except urllib.error.HTTPError as error:
            # only the fixed GitHub archive endpoint may redirect; never forward its credential
            if redirect and error.code == 302:
                location = error.headers.get("Location", "")
                error.close()
                parts = urllib.parse.urlsplit(location)
                require(parts.scheme == "https" and parts.hostname and not parts.username and not parts.password
                        and parts.port in (None, 443) and not parts.fragment, "invalid artifact redirect")
                return self.open(location)
            error.close()
            raise Rejected("artifact HTTP request failed: " + str(error.code)) from None
        if response.status != 200:
            response.close()
            raise Rejected("unexpected artifact response")
        return response

    def copy(self, source, destination, limit, digest=None):
        size = 0
        while True:
            self.remaining()
            block = source.read(min(1024 * 1024, limit + 1 - size))
            if not block:
                break
            size += len(block)
            require(size <= limit, "artifact exceeds byte limit")
            if digest is not None:
                digest.update(block)
            destination.write(block)
        return size

    def metadata(self, url):
        with self.open(url, authenticated=True) as response:
            data = response.read(MAX_METADATA + 1)
        require(len(data) <= MAX_METADATA, "artifact metadata exceeds byte limit")
        return json.loads(data)


def validate_metadata(data, artifact_id, kind, repository_id, run_id, attempt, controller, source):
    require(isinstance(data, dict), "invalid artifact metadata")
    require(type(data.get("id")) is int and data["id"] == artifact_id, "artifact ID mismatch")
    require(data.get("expired") is False, "artifact expired or unavailable")
    name = re.fullmatch(r"release-" + re.escape(kind + "-" + source) + r"-([1-9][0-9]{0,9})", str(data.get("name", "")))
    require(name and int(name[1]) <= attempt, "artifact name or attempt mismatch")
    run = data.get("workflow_run")
    require(isinstance(run, dict), "missing artifact run identity")
    for key, expected in (("id", run_id), ("repository_id", repository_id), ("head_repository_id", repository_id)):
        require(type(run.get(key)) is int and run[key] == expected, "artifact run or repository mismatch")
    require(run.get("head_sha") == controller, "artifact controller mismatch")
    size = data.get("size_in_bytes")
    require(type(size) is int and 0 < size <= MAX_ARCHIVE, "invalid artifact archive size")
    digest = data.get("digest")
    require(isinstance(digest, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", digest), "missing artifact SHA256")
    return size, digest[7:]


def unpack(client, archive, destination, filename):
    # read one fixed file rather than extracting paths supplied by the archive
    with zipfile.ZipFile(archive) as bundle:
        entries = bundle.infolist()
        require(len(entries) == 1, "artifact must contain exactly one asset")
        info = entries[0]
        mode = info.external_attr >> 16
        require(info.filename == filename and info.orig_filename == filename and not info.is_dir(), "unexpected asset path")
        require(stat.S_IFMT(mode) in (0, stat.S_IFREG), "asset is not a regular file")
        require(not info.flag_bits & 1 and info.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED), "unsupported asset encoding")
        require(0 < info.file_size <= MAX_ASSET, "invalid asset size")
        with bundle.open(info) as source, destination.open("xb") as target:
            size = client.copy(source, target, MAX_ASSET)
        require(size == info.file_size, "asset size mismatch")
    # the inner package ZIP is uploaded as data; never extract or execute packages
    require(zipfile.is_zipfile(destination), "asset is not a ZIP bundle")


def stage(env, client):
    repository = env.get("GITHUB_REPOSITORY", "")
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*", repository), "invalid repository")
    repository_id = number(env.get("GITHUB_REPOSITORY_ID", ""))
    run_id, attempt = number(env.get("GITHUB_RUN_ID", "")), number(env.get("GITHUB_RUN_ATTEMPT", ""))
    controller, source, tag = (env.get(k, "") for k in ("GITHUB_SHA", "RELEASE_COMMIT", "RELEASE_TAG"))
    require(re.fullmatch(r"[0-9a-f]{40}", controller) and re.fullmatch(r"[0-9a-f]{40}", source), "invalid commit identity")
    require(re.fullmatch(r"v[0-9][0-9A-Za-z.+-]{0,126}", tag), "invalid release tag")
    ids = [number(env.get(kind.replace("-", "_").upper() + "_ID", "")) for kind in KINDS]
    require(len(set(ids)) == len(KINDS), "duplicate artifact IDs")
    root = Path(env["RUNNER_TEMP"])
    directory = Path(tempfile.mkdtemp(prefix="release-assets-", dir=root))
    try:
        for kind, artifact_id in zip(KINDS, ids):
            endpoint = f"{API}/repos/{repository}/actions/artifacts/{artifact_id}"
            size, digest = validate_metadata(client.metadata(endpoint), artifact_id, kind, repository_id,
                                             run_id, attempt, controller, source)
            with tempfile.TemporaryFile(dir=root) as archive:
                with client.open(endpoint + "/zip", authenticated=True, redirect=True) as response:
                    actual_digest = hashlib.sha256()
                    actual_size = client.copy(response, archive, MAX_ARCHIVE, digest=actual_digest)
                require((actual_size, actual_digest.hexdigest()) == (size, digest), "artifact archive size or SHA256 mismatch")
                archive.seek(0)
                filename = f"rcp-{kind}-{tag}.zip"
                unpack(client, archive, directory / filename, filename)
        client.remaining()
        return directory
    except BaseException:
        shutil.rmtree(directory)
        raise


def main():
    token = os.environ.get("GH_TOKEN", "")
    require(token and not any(c in token for c in "\r\n"), "missing artifact read credential")
    require(os.environ.get("RUNNER_TEMP"), "missing runner temporary directory")
    require(os.environ.get("GITHUB_OUTPUT"), "missing workflow output path")
    directory = stage(os.environ, Client(token))
    try:
        # the next release step runs only after all four assets and output publication succeed
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            require(not any(c in str(directory) for c in "\r\n"), "invalid staging directory")
            output.write("directory=" + str(directory) + "\n")
    except BaseException:
        shutil.rmtree(directory)
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # only our authored validation messages are safe; other errors may contain signed URLs
        reason = str(error) if isinstance(error, Rejected) else "unexpected staging error"
        print("::error::Release artifact staging failed: " + reason + "; no release upload authorized", file=sys.stderr)
        sys.exit(1)
