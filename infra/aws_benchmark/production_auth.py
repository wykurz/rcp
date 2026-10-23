"""Verify the separate GitHub OIDC bearer token used for production admission.

This module authenticates signed run identity only. Authorization and persistent
run deduplication belong to admission policy. Never log the token or request body.
"""

import base64
from dataclasses import dataclass
import http.client
import math
import re
import ssl
import threading
import time
from collections.abc import Callable, Collection

from benchmarks.strict_json import parse_json
from .production_transport import retryable_transport

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa


ISSUER = "https://token.actions.githubusercontent.com"
MAIN_REF = "refs/heads/main"
MAX_TOKEN_BYTES = 16 * 1024
MAX_JWKS_BYTES = 64 * 1024
MAX_JWKS_KEYS = 16
MAX_TOKEN_LIFETIME_SECONDS = 600
MAX_TOKEN_AGE_SECONDS = 600
CLOCK_SKEW_SECONDS = 60
JWKS_CACHE_SECONDS = 300
JWKS_REFRESH_SECONDS = 30
JWKS_TIMEOUT_SECONDS = 3
_ID = re.compile(r"[1-9][0-9]{0,19}")
_SHA = re.compile(r"[0-9a-f]{40}")
_KEY_ID = re.compile(r"[A-Za-z0-9_.-]{1,128}")
_REPOSITORY = re.compile(r"[A-Za-z0-9-]{1,100}/[A-Za-z0-9_.-]{1,100}")
_REQUIRED = ("iss", "aud", "sub", "exp", "iat", "nbf", "jti", "repository",
             "repository_id", "repository_owner", "repository_owner_id", "ref",
             "workflow_ref", "workflow_sha", "sha", "run_id", "run_attempt",
             "actor", "actor_id", "event_name")


class GitHubIdentityError(ValueError):
    """A token or trusted key set could not be verified; messages exclude input."""


class GitHubIdentityTransientError(GitHubIdentityError):
    """A sanitized key-source outage; no unverified identity may be returned."""

    def __init__(self, retry_after_seconds=JWKS_REFRESH_SECONDS):
        super().__init__("GitHub signing key temporarily unavailable")
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True, slots=True)
class VerifiedGitHubIdentity:
    """Identity derived exclusively from validated, signed GitHub claims."""

    repository: str
    repository_id: str
    repository_owner: str
    repository_owner_id: str
    ref: str
    workflow_ref: str
    workflow_sha: str
    sha: str
    run_id: str
    run_attempt: str
    actor: str
    actor_id: str
    event_name: str
    jti: str
    issued_at: int
    expires_at: int

    @property
    def run_key(self) -> tuple[str, str]:
        """Deduplicate all attempts of one run in one immutable repository."""
        return self.repository_id, self.run_id


def _require(condition):
    if not condition:
        raise GitHubIdentityError("invalid GitHub admission token")


def _matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _json(raw):
    value = parse_json(raw.decode("utf-8"))
    _require(isinstance(value, dict))
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        _require(depth <= 8)
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
    return value


def _segment(value):
    _require(re.fullmatch(r"[A-Za-z0-9_-]+", value) is not None)
    decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    _require(base64.urlsafe_b64encode(decoded).decode().rstrip("=") == value)
    return decoded


def _fetch_official_jwks() -> bytes:
    """Read bounded key bytes with a socket timeout and elapsed body checks.

    DNS resolution and header parsing are not hard wall-clock bounded by the
    socket timeout. Admission must run under a finite outer Lambda execution
    deadline. A late response fails its next elapsed-time check, and token
    expiration is checked again after key retrieval and signature verification.
    """
    # httpsconnection ignores proxy environment variables and does not follow redirects
    connection = http.client.HTTPSConnection("token.actions.githubusercontent.com",
        timeout=JWKS_TIMEOUT_SECONDS, context=ssl.create_default_context())
    deadline = time.monotonic() + JWKS_TIMEOUT_SECONDS
    response = None
    try:
        connection.request("GET", "/.well-known/jwks", headers={"Accept": "application/json"})
        transport = connection.sock
        response = connection.getresponse()
        if response.status == 429 or 500 <= response.status < 600:
            raise GitHubIdentityTransientError()
        _require(response.status == 200)
        _require(response.getheader("Content-Encoding", "identity") == "identity")
        chunks = []
        remaining = MAX_JWKS_BYTES + 1
        while remaining:
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                raise GitHubIdentityTransientError()
            # read1 performs at most one underlying read; reset to the remaining budget
            if transport is not None and not response.isclosed():
                transport.settimeout(timeout)
            chunk = response.read1(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        _require(remaining > 0)
        if response.length not in (None, 0):
            raise GitHubIdentityTransientError()
        if time.monotonic() > deadline:
            raise GitHubIdentityTransientError()
        return b"".join(chunks)
    except Exception as error:
        if retryable_transport(error):
            raise GitHubIdentityTransientError() from None
        raise
    finally:
        if response is not None:
            response.close()
        connection.close()


class GitHubIdentityVerifier:
    """Verify tokens against fixed trust settings with a bounded in-memory key cache.

    The injected clock returns Unix seconds. The optional zero-argument JWKS fetcher
    returns raw UTF-8 bytes and is for trusted integration/tests, never request data.
    Time validation uses the injected clock instead of PyJWT's process-global clock.
    A reused instance refreshes at most once per 30 seconds, including failed fetches.
    """

    def __init__(self, *, repository: str, repository_id: str, repository_owner_id: str,
                 audience: str, workflow_refs: Collection[str],
                 clock: Callable[[], float] = time.time,
                 jwks_fetcher: Callable[[], bytes] | None = None):
        _require(_matches(_REPOSITORY, repository))
        _require(_matches(_ID, repository_id) and _matches(_ID, repository_owner_id))
        _require(isinstance(audience, str) and 0 < len(audience) <= 512
                 and audience.isascii() and not any(char.isspace() for char in audience)
                 and audience != "sts.amazonaws.com")
        _require(not isinstance(workflow_refs, str))
        refs = frozenset(workflow_refs)
        _require(0 < len(refs) <= 8)
        prefix = re.escape(repository) + r"/\.github/workflows/[A-Za-z0-9_-]+\.ya?ml@"
        _require(all(isinstance(ref, str) and re.fullmatch(prefix + MAIN_REF, ref) for ref in refs))
        self._repository = repository
        self._repository_id = repository_id
        self._owner_id = repository_owner_id
        self._audience = audience
        self._workflow_refs = refs
        self._clock = clock
        self._fetch = jwks_fetcher if jwks_fetcher is not None else _fetch_official_jwks
        self._keys = {}
        self._fetched_at = None
        self._last_refresh = None
        self._refresh_transient = False
        self._lock = threading.Lock()

    def verify(self, token: str) -> VerifiedGitHubIdentity:
        """Return verified identity, or raise a sanitized GitHubIdentityError."""
        try:
            return self._verify(token)
        except GitHubIdentityTransientError:
            raise
        except (jwt.PyJWTError, ValueError, TypeError, UnicodeError, RecursionError,
                OverflowError):
            raise GitHubIdentityError("invalid GitHub admission token") from None

    def _verify(self, token):
        _require(isinstance(token, str) and 0 < len(token) <= MAX_TOKEN_BYTES and token.isascii())
        parts = token.split(".")
        _require(len(parts) == 3)
        header = _json(_segment(parts[0]))
        claims = _json(_segment(parts[1]))
        signature = _segment(parts[2])
        _require(256 <= len(signature) <= 512)
        # key and algorithm selection never come from URLs or embedded caller keys
        _require(set(header) <= {"alg", "typ", "kid", "x5t"})
        _require(header.get("alg") == "RS256" and header.get("typ") == "JWT")
        _require(_matches(_KEY_ID, header.get("kid")))
        _require("x5t" not in header or _matches(_KEY_ID, header["x5t"]))
        now = self._now()
        self._validate_claims(claims, now)
        key = self._key(header["kid"], now)
        verified = jwt.decode(token, key=key, algorithms=["RS256"], issuer=ISSUER,
            audience=self._audience, options={"require": list(_REQUIRED), "strict_aud": True,
                "verify_exp": False, "verify_iat": False, "verify_nbf": False})
        _require(verified == claims)
        # key retrieval and signature verification must not carry a token past expiry
        self._validate_claims(verified, self._now())
        return VerifiedGitHubIdentity(**{name: claims[name] for name in (
            "repository", "repository_id", "repository_owner", "repository_owner_id", "ref",
            "workflow_ref", "workflow_sha", "sha", "run_id", "run_attempt", "actor", "actor_id",
            "event_name", "jti")}, issued_at=claims["iat"], expires_at=claims["exp"])

    def _now(self):
        now = self._clock()
        _require(type(now) in (int, float) and math.isfinite(now) and now > 0)
        return now

    def _validate_claims(self, claims, now):
        _require(all(name in claims for name in _REQUIRED))
        _require(claims["iss"] == ISSUER and claims["aud"] == self._audience)
        _require(isinstance(claims["sub"], str) and 0 < len(claims["sub"]) <= 1024)
        _require(all(type(claims[name]) is int and 0 < claims[name] < 2**53
                     for name in ("iat", "nbf", "exp")))
        issued, before, expires = (claims[name] for name in ("iat", "nbf", "exp"))
        # github backdates nbf independently of iat (including by 600s in its docs).
        # tolerate bounded forward clock skew, but never extend expiration or token age.
        _require(before <= now + CLOCK_SKEW_SECONDS and before < expires)
        _require(issued <= now + CLOCK_SKEW_SECONDS and now - issued < MAX_TOKEN_AGE_SECONDS)
        _require(now < expires and 0 < expires - issued <= MAX_TOKEN_LIFETIME_SECONDS)
        _require(_matches(_KEY_ID, claims["jti"]))
        _require(claims["repository"] == self._repository
                 and claims["repository_id"] == self._repository_id
                 and claims["repository_owner"] == self._repository.split("/")[0]
                 and claims["repository_owner_id"] == self._owner_id)
        _require(claims["ref"] == MAIN_REF
                 and isinstance(claims["workflow_ref"], str)
                 and claims["workflow_ref"] in self._workflow_refs)
        _require(_matches(_SHA, claims["sha"]) and claims["workflow_sha"] == claims["sha"])
        _require(all(_matches(_ID, claims[name]) for name in ("run_id", "run_attempt", "actor_id")))
        _require(isinstance(claims["actor"], str)
                 and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_\[\]-]{0,99}", claims["actor"]))
        _require(isinstance(claims["event_name"], str)
                 and claims["event_name"] in {"workflow_dispatch", "schedule"})

    def _key(self, kid, now):
        with self._lock:
            fresh = self._fetched_at is not None and 0 <= now - self._fetched_at < JWKS_CACHE_SECONDS
            if fresh and kid in self._keys:
                return self._keys[kid]
            if self._last_refresh is not None and now - self._last_refresh < JWKS_REFRESH_SECONDS:
                if self._refresh_transient and now >= self._last_refresh:
                    raise GitHubIdentityTransientError(JWKS_REFRESH_SECONDS)
                raise GitHubIdentityError("GitHub signing key unavailable")
            self._last_refresh = now
            self._refresh_transient = False
            # a failed refresh invalidates the previous set; never use stale keys
            self._keys = {}
            self._fetched_at = None
            try:
                self._keys = self._parse_keys(self._fetch())
            except Exception as error:
                if isinstance(error, GitHubIdentityTransientError) or retryable_transport(error):
                    self._refresh_transient = True
                    raise GitHubIdentityTransientError() from None
                raise GitHubIdentityError("GitHub signing key unavailable") from None
            self._fetched_at = now
            _require(kid in self._keys)
            return self._keys[kid]

    @staticmethod
    def _parse_keys(raw):
        _require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_JWKS_BYTES)
        document = _json(raw)
        entries = document.get("keys")
        _require(isinstance(entries, list) and 0 < len(entries) <= MAX_JWKS_KEYS)
        keys = {}
        for entry in entries:
            _require(isinstance(entry, dict) and _matches(_KEY_ID, entry.get("kid")))
            _require(entry["kid"] not in keys and entry.get("kty") == "RSA"
                     and entry.get("use") == "sig" and entry.get("alg", "RS256") == "RS256")
            _require(entry.get("key_ops", ["verify"]) == ["verify"])
            _require(not set(entry) & {"d", "p", "q", "dp", "dq", "qi", "oth", "jku", "x5u"})
            _require(isinstance(entry.get("n"), str) and 0 < len(entry["n"]) <= 684)
            _require(isinstance(entry.get("e"), str) and 0 < len(entry["e"]) <= 8)
            key = jwt.PyJWK.from_dict(entry, algorithm="RS256").key
            _require(isinstance(key, rsa.RSAPublicKey) and 2048 <= key.key_size <= 4096
                     and key.public_numbers().e == 65537)
            keys[entry["kid"]] = key
        return keys
