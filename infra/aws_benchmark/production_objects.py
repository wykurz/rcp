"""Bounded SHA-256/version-bound object evidence shared by watchdog and collector."""

import base64

from .common import digest, require


class Objects:
    """Bounded versioned reads and create-only writes, with reconciliation after lost responses."""
    def __init__(self, api, bucket):
        self.api, self.bucket = api, bucket

    def read(self, key, limit, *, version=None, optional=False):
        args = dict(Bucket=self.bucket, Key=key, ChecksumMode="ENABLED")
        if version is not None:
            args["VersionId"] = version
        if optional:
            listing = self.api.list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=1)
            contents = listing.get("Contents", [])
            require(isinstance(contents, list) and len(contents) <= 1 and
                    type(listing.get("KeyCount")) is int and listing["KeyCount"] == len(contents),
                    "optional object listing is incomplete")
            if not contents:
                require(listing.get("IsTruncated") is False and not listing.get("NextContinuationToken"),
                        "empty object listing is truncated")
                return None
            require(contents[0].get("Key") == key, "optional object listing differs")
        # a listed object must be readable; no 403 or subsequent 404 becomes absence
        response = self.api.get_object(**args)
        body = response["Body"]
        try:
            size, found = response.get("ContentLength"), response.get("VersionId")
            require(type(size) is int and 0 < size <= limit and isinstance(found, str) and
                    found not in ("", "null") and len(found) <= 1024 and
                    all(32 < ord(char) < 127 for char in found) and
                    (version is None or found == version) and response.get("ServerSideEncryption") == "AES256",
                    "object size, version or encryption differs")
            raw = body.read(limit + 1)
            require(isinstance(raw, bytes) and len(raw) == size and body.read(1) == b"",
                    "object read is incomplete")
            sha256 = digest(raw)
            checksum = base64.b64encode(bytes.fromhex(sha256)).decode()
            require(response.get("ChecksumSHA256") == checksum and
                    response.get("ChecksumType", "FULL_OBJECT") == "FULL_OBJECT", "object checksum differs")
        finally:
            body.close()
        return raw, dict(key=key, version=found, sha256=sha256, bytes=len(raw))

    def head(self, key, limit, *, sha256):
        """Commit exact immutable source metadata; read() still verifies its bytes later."""
        response = self.api.head_object(Bucket=self.bucket, Key=key, ChecksumMode="ENABLED")
        size, version = response.get("ContentLength"), response.get("VersionId")
        require(type(size) is int and 0 < size <= limit and isinstance(version, str) and
                version not in ("", "null") and len(version) <= 1024 and
                all(32 < ord(char) < 127 for char in version) and
                response.get("ServerSideEncryption") == "AES256" and
                response.get("ChecksumType", "FULL_OBJECT") == "FULL_OBJECT" and
                response.get("ChecksumSHA256") == base64.b64encode(bytes.fromhex(sha256)).decode(),
                "object commitment metadata differs")
        return dict(key=key, version=version, sha256=sha256, bytes=size)

    def once(self, key, raw, limit):
        require(isinstance(raw, bytes) and 0 < len(raw) <= limit, "retained object exceeds bound")
        current = self.read(key, limit, optional=True)
        if current is None:
            try:
                self.api.put_object(Bucket=self.bucket, Key=key, Body=raw, IfNoneMatch="*",
                    ServerSideEncryption="AES256", ChecksumSHA256=base64.b64encode(bytes.fromhex(digest(raw))).decode())
            except Exception:
                # never replay a possibly successful write; exact read-back resolves it or holds
                current = self.read(key, limit, optional=True)
                require(current is not None, "retained write has uncertain outcome")
            else:
                current = self.read(key, limit)
        require(current[0] == raw, "retained object already contains different evidence")
        exact = self.read(key, limit, version=current[1]["version"])
        require(exact == current, "retained object version changed")
        return exact[1]

    def referenced(self, ref, key, limit):
        require(isinstance(ref, dict) and set(ref) == {"key", "version", "sha256", "bytes"} and ref["key"] == key,
                "retained reference belongs to another object")
        raw, actual = self.read(key, limit, version=ref["version"])
        require(actual == ref, "retained reference digest differs")
        return raw
