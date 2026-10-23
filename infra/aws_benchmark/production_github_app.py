"""Scoped GitHub App authentication for fresh, fixed-path producer observations.

Only admission/collection use this module. Runtime configuration is non-secret;
AWSCURRENT holds the PEM. Neither PEM/JWT nor producer proofs are cached. Tokens
remain opaque and memory-only. Python cannot promise secure memory erasure.
Socket timeouts cannot hard-bound DNS/header parsing; the outer Lambda deadline
remains required. A late operation fails its next shared-budget check.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
import http.client
import json
import logging
import math
import re
import ssl
import threading
import time

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from .production_transport import retryable_transport
from .production_github import (API_HOST, API_VERSION, MAX_RESPONSE_BYTES,
                                GitHubRunError, GitHubRateLimitError, GitHubTransientError, _bounded_document)

OBSERVATION_SECONDS = 20
MAX_CALLS = 17
SOCKET_SECONDS = 3
MAX_PEM_BYTES = 16384
PERMISSIONS = {"actions": "read", "metadata": "read"}
REPOSITORIES_PATH = "/installation/repositories?per_page=2"
_ID = r"[1-9][0-9]{0,19}"


def _require(value):
    if not value:
        raise GitHubRunError("GitHub App reader unavailable")


def _number(value):
    _require(type(value) in (int, float) and math.isfinite(value) and value >= 0)
    return value


@dataclass(frozen=True)
class GitHubAppConfig:
    repository: str
    repository_id: str
    repository_owner_id: str
    region: str
    account: str
    app_client_id: str
    installation_id: str
    secret_arn: str

    @classmethod
    def from_environment(cls, environment, *, region, account):
        """Copy only non-secret deployment pins; construct no clients or credentials."""
        _require(isinstance(environment, Mapping))
        return cls(repository=environment.get("RCP_PRODUCTION_REPOSITORY"),
            repository_id=environment.get("RCP_PRODUCTION_REPOSITORY_ID"),
            repository_owner_id=environment.get("RCP_PRODUCTION_OWNER_ID"), region=region, account=account,
            app_client_id=environment.get("RCP_PRODUCTION_GITHUB_APP_CLIENT_ID"),
            installation_id=environment.get("RCP_PRODUCTION_GITHUB_APP_INSTALLATION_ID"),
            secret_arn=environment.get("RCP_PRODUCTION_GITHUB_APP_SECRET_ARN"))

    def __post_init__(self):
        _require(all(isinstance(value, str) for value in self.__dict__.values()))
        _require(re.fullmatch(r"[A-Za-z0-9-]{1,100}/[A-Za-z0-9_.-]{1,100}", self.repository)
                 and self.repository.split("/")[1] not in {".", ".."})
        _require(all(re.fullmatch(_ID, value) for value in
                     (self.repository_id, self.repository_owner_id, self.installation_id)))
        _require(re.fullmatch(r"[a-z]{2}-[a-z]+-[1-9][0-9]?", self.region)
                 and re.fullmatch(r"[0-9]{12}", self.account))
        _require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", self.app_client_id))
        _require(re.fullmatch(r"arn:aws:secretsmanager:" + re.escape(self.region + ":" + self.account)
            + r":secret:/rcp/production/github-read-app-[A-Za-z0-9]{6}", self.secret_arn))


@dataclass(frozen=True, repr=False)
class _Token:
    value: str
    minted_wall: float
    minted_mono: float
    until_wall: float
    until_mono: float

    def __repr__(self):
        return "<opaque GitHub installation token>"


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict
    body: bytes = field(repr=False)


class _NoSdkDebug(logging.Filter):
    def filter(self, record):
        return record.levelno > logging.DEBUG


_SDK_FILTER = _NoSdkDebug()


def _suppress_sdk_debug():
    # botocore's parser DEBUG records include SecretString; keep application logs intact.
    # newly created SDK loggers inherit INFO; existing configured children also get a filter.
    prefixes = ("botocore", "boto3", "urllib3")
    names = set(prefixes) | set(logging.Logger.manager.loggerDict)
    for name in names:
        if any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes):
            logger = logging.getLogger(name)
            logger.setLevel(max(logging.INFO, logger.level))
            if _SDK_FILTER not in logger.filters:
                logger.addFilter(_SDK_FILTER)


def _https(method, path, headers, body, *, timeout, deadline, monotonic):
    connection = http.client.HTTPSConnection(API_HOST, timeout=timeout, context=ssl.create_default_context())
    connection.set_debuglevel(0)
    response = None
    try:
        connection.request(method, path, body=body, headers=headers)
        transport = connection.sock
        response = connection.getresponse()
        selected = {name: response.getheader(name) for name in
                    ("content-encoding", "link", "retry-after", "x-ratelimit-remaining", "x-ratelimit-reset")}
        _require(selected["content-encoding"] in (None, "identity"))
        remaining, chunks = MAX_RESPONSE_BYTES + 1, []
        while remaining:
            left = deadline - _number(monotonic())
            if left <= 0:
                raise GitHubTransientError()
            if transport is not None and not response.isclosed():
                transport.settimeout(min(SOCKET_SECONDS, left))
            chunk = response.read1(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        _require(remaining > 0)
        if response.length not in (None, 0):
            raise GitHubTransientError()
        if _number(monotonic()) >= deadline:
            raise GitHubTransientError()
        return Response(response.status, selected, b"".join(chunks))
    finally:
        if response is not None:
            response.close()
        connection.close()


class GitHubAppProvider:
    """An immutable-scope token cache; constructing it performs no SDK/network call."""

    def __init__(self, config, *, secrets=None, transport=_https, clock=time.time, monotonic=time.monotonic):
        _require(isinstance(config, GitHubAppConfig))
        self.config, self._secrets, self._transport = config, secrets, transport
        self._clock, self._monotonic = clock, monotonic
        self._cached = None
        self._lock = threading.Lock()
        self._quota_wall = self._quota_mono = 0
        self._path = re.compile(r"/repos/" + re.escape(config.repository) + r"/actions/runs/" + _ID
            + r"(?:/attempts/" + _ID + r"(?:/jobs\?per_page=100)?|/pending_deployments)?")
        self._installation_path = "/app/installations/" + config.installation_id
        self._mint_path = self._installation_path + "/access_tokens"

    def observation(self):
        return _Observation(self)

    def _usable(self, token):
        wall, mono = _number(self._clock()), _number(self._monotonic())
        return token is not None and token.minted_wall <= wall < token.until_wall \
            and token.minted_mono <= mono < token.until_mono

    def _token(self, observation):
        self._check_quota()
        if not self._lock.acquire(timeout=observation.remaining()):
            raise GitHubTransientError()
        try:
            observation.check()
            if not self._usable(self._cached):
                self._cached = None
                self._cached = self._mint(observation)
            return self._cached
        finally:
            self._lock.release()

    def _invalidate(self, token, observation):
        # do not evict a newer token minted by a concurrent observation.
        if not self._lock.acquire(timeout=observation.remaining()):
            raise GitHubTransientError()
        try:
            observation.check()
            if self._cached is token:
                self._cached = None
        finally:
            self._lock.release()

    def _secret(self, observation):
        observation.step()
        _suppress_sdk_debug()
        try:
            if self._secrets is None:
                import boto3
                from botocore.config import Config
                self._secrets = boto3.client("secretsmanager", region_name=self.config.region,
                    endpoint_url=f"https://secretsmanager.{self.config.region}.amazonaws.com",
                    config=Config(connect_timeout=1, read_timeout=3, retries={"total_max_attempts": 1},
                                  proxies={}, ignore_configured_endpoint_urls=True))
            _suppress_sdk_debug()
            result = self._secrets.get_secret_value(SecretId=self.config.secret_arn, VersionStage="AWSCURRENT")
        except Exception as error:
            if retryable_transport(error):
                raise GitHubTransientError() from None
            raise
        observation.check()
        _require(isinstance(result, dict) and result.get("ARN") == self.config.secret_arn
            and result.get("Name") == "/rcp/production/github-read-app"
            and isinstance(result.get("VersionId"), str) and re.fullmatch(r"[A-Za-z0-9-]{32,64}", result["VersionId"])
            and isinstance(result.get("VersionStages"), list) and "AWSCURRENT" in result["VersionStages"]
            and "SecretBinary" not in result)
        pem = result.get("SecretString")
        _require(isinstance(pem, str) and pem.isascii() and 0 < len(pem) <= MAX_PEM_BYTES)
        return pem

    def _mint(self, observation):
        pem = self._secret(observation)
        try:
            key = serialization.load_pem_private_key(pem.encode("ascii"), password=None)
            _require(isinstance(key, rsa.RSAPrivateKey) and 2048 <= key.key_size <= 4096
                     and key.public_key().public_numbers().e == 65537)
            wall, mono = _number(self._clock()), _number(self._monotonic())
            _require(wall > 60)
            bearer = jwt.encode(dict(iat=int(wall) - 60, exp=int(wall) + 300, iss=self.config.app_client_id),
                                key, algorithm="RS256")
        finally:
            del pem
        del key
        body = json.dumps(dict(repository_ids=[int(self.config.repository_id)], permissions=PERMISSIONS),
                          separators=(",", ":"), sort_keys=True).encode()
        try:
            self._verify_app(observation, bearer)
            response = self._request(observation, "POST", self._mint_path, bearer, body)
        finally:
            del bearer
        _require(response.status == 201)
        value = _bounded_document(response.body)
        _require(isinstance(value, dict) and value.get("permissions") == PERMISSIONS
                 and value.get("repository_selection") == "selected")
        opaque, expiry = value.get("token"), value.get("expires_at")
        _require(isinstance(opaque, str) and 0 < len(opaque) <= 8192 and all(32 < ord(c) < 127 for c in opaque)
                 and isinstance(expiry, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", expiry))
        expires = datetime.fromisoformat(expiry.replace("Z", "+00:00")).timestamp()
        _require(300 < expires - wall <= 3660)
        scoped = self._request(observation, "GET", REPOSITORIES_PATH, opaque)
        _require(scoped.status == 200)
        inventory = _bounded_document(scoped.body)
        _require(isinstance(inventory, dict) and type(inventory.get("total_count")) is int
                 and inventory["total_count"] == 1 and isinstance(inventory.get("repositories"), list)
                 and len(inventory["repositories"]) == 1)
        repository = inventory["repositories"][0]
        _require(isinstance(repository, dict) and type(repository.get("id")) is int
                 and repository["id"] == int(self.config.repository_id)
                 and repository.get("full_name") == self.config.repository)
        owner = repository.get("owner")
        _require(isinstance(owner, dict) and type(owner.get("id")) is int
                 and owner["id"] == int(self.config.repository_owner_id)
                 and owner.get("login") == self.config.repository.split("/")[0])
        until = min(expires - 300, wall + 3300)
        token = _Token(opaque, wall, mono, until, mono + until - wall)
        _require(self._usable(token))
        return token

    def _verify_app(self, observation, bearer):
        def read(path):
            response = self._request(observation, "GET", path, bearer)
            _require(response.status == 200)
            value = _bounded_document(response.body)
            _require(isinstance(value, dict) and value.get("permissions") == PERMISSIONS
                     and value.get("events") == [])
            return value
        def exact_owner(owner):
            return (isinstance(owner, dict) and type(owner.get("id")) is int
                and owner["id"] == int(self.config.repository_owner_id)
                and owner.get("login") == self.config.repository.split("/")[0])
        application = read("/app")
        app_id = application.get("id")
        _require(type(app_id) is int and 0 < app_id < 10 ** 20
                 and application.get("client_id") == self.config.app_client_id
                 and exact_owner(application.get("owner"))
                 and type(application.get("installations_count")) is int
                 and application["installations_count"] == 1)
        installation = read(self._installation_path)
        _require(type(installation.get("id")) is int
                 and installation["id"] == int(self.config.installation_id)
                 and type(installation.get("app_id")) is int and installation["app_id"] == app_id
                 and exact_owner(installation.get("account"))
                 and type(installation.get("target_id")) is int
                 and installation["target_id"] == int(self.config.repository_owner_id)
                 and installation.get("repository_selection") == "selected"
                 and "suspended_at" in installation and installation["suspended_at"] is None
                 and "suspended_by" in installation and installation["suspended_by"] is None)

    def _check_quota(self):
        wall, mono = _number(self._clock()), _number(self._monotonic())
        delay = max(self._quota_wall - wall, self._quota_mono - mono)
        if delay > 0:
            raise GitHubRateLimitError(min(86400, math.ceil(delay)))

    def _request(self, observation, method, path, bearer, body=None):
        _require((method == "POST" and path == self._mint_path and isinstance(body, bytes))
                 or (method == "GET" and body is None
                     and (path in ("/app", self._installation_path, REPOSITORIES_PATH) or self._path.fullmatch(path))))
        self._check_quota()
        timeout = observation.step()
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": API_VERSION,
                   "User-Agent": "rcp-production-reader", "Authorization": "Bearer " + bearer}
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            response = self._transport(method, path, headers, body, timeout=min(SOCKET_SECONDS, timeout),
                                       deadline=observation.deadline, monotonic=self._monotonic)
        except Exception as error:
            if retryable_transport(error):
                raise GitHubTransientError() from None
            raise
        observation.check()
        _require(isinstance(response, Response) and type(response.status) is int and isinstance(response.headers, dict)
                 and isinstance(response.body, bytes) and len(response.body) <= MAX_RESPONSE_BYTES
                 and response.headers.get("content-encoding") in (None, "identity")
                 and not response.headers.get("link"))
        quota = response.status == 429
        if response.status == 403:
            quota = response.headers.get("x-ratelimit-remaining") == "0" or bool(response.headers.get("retry-after"))
            if not quota:
                try:
                    value = _bounded_document(response.body)
                    message = value.get("message", "") if isinstance(value, dict) else ""
                    quota = isinstance(message, str) and (message.startswith("API rate limit exceeded")
                        or message.startswith("You have exceeded a secondary rate limit")
                        or message.startswith("You have exceeded a rate limit"))
                except Exception:
                    pass
        if quota:
            def number(name):
                value = response.headers.get(name)
                return int(value) if isinstance(value, str) and re.fullmatch(r"[0-9]{1,12}", value) else 0
            wall, mono = _number(self._clock()), _number(self._monotonic())
            reset = number("x-ratelimit-reset") - wall + 1 if response.headers.get("x-ratelimit-remaining") == "0" else 0
            delay = max(60, number("retry-after"), reset)
            self._quota_wall, self._quota_mono = wall + delay, mono + delay
            raise GitHubRateLimitError(min(86400, math.ceil(delay)))
        if 500 <= response.status < 600:
            raise GitHubTransientError()
        return response


class _Observation:
    """One producer proof: fixed deadline/call count and at most one token refresh."""

    def __init__(self, provider):
        self.provider, self.deadline, self.calls = provider, None, 0
        self._current, self._refreshed = None, False

    def __enter__(self):
        _require(self.deadline is None)
        self.deadline = _number(self.provider._monotonic()) + OBSERVATION_SECONDS
        return self

    def __exit__(self, kind, value, traceback):
        self._current = None
        if kind is None:
            self.check()
        self.deadline = 0

    def remaining(self):
        _require(self.deadline is not None and self.deadline > 0)
        left = self.deadline - _number(self.provider._monotonic())
        if left <= 0:
            raise GitHubTransientError()
        return left

    def check(self):
        self.remaining()

    def step(self):
        left = self.remaining()
        _require(self.calls < MAX_CALLS)
        self.calls += 1
        return left

    def _refresh(self):
        _require(not self._refreshed)
        self._refreshed = True
        self.provider._invalidate(self._current, self)
        self._current = self.provider._token(self)

    def __call__(self, path):
        try:
            _require(isinstance(path, str) and self.provider._path.fullmatch(path))
            self.check()
            if self._current is None:
                self._current = self.provider._token(self)
            elif not self.provider._usable(self._current):
                self._refresh()
            response = self.provider._request(self, "GET", path, self._current.value)
            if response.status == 401:
                self._refresh()
                response = self.provider._request(self, "GET", path, self._current.value)
            _require(response.status == 200)
            return response.body
        except (GitHubRateLimitError, GitHubTransientError):
            raise
        except Exception:
            raise GitHubRunError("GitHub App reader unavailable") from None


@lru_cache(maxsize=1)
def cached_provider(config):
    """Reuse only the exact deployment tuple's opaque token, never observations."""
    return GitHubAppProvider(config)
