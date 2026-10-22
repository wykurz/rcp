"""Read and bind public GitHub run metadata to a verified admission identity.

The attempt endpoint's created_at is the attempt creation time on reruns. The
run endpoint preserves the original creation time, so both responses are checked
and only the run endpoint supplies the admission age proof.

REST contract: https://docs.github.com/en/rest/actions/workflow-runs
The socket timeout and body deadline checks are not a hard wall-clock bound on
DNS resolution or HTTP headers. The caller must enforce a finite outer Lambda
execution timeout. No request is retried, and failures never yield an age proof.
"""

from collections.abc import Callable
from datetime import datetime, timezone
import http.client
import math
import re
import ssl
import time

from benchmarks.strict_json import parse_json
from .production_auth import VerifiedGitHubIdentity
from .production_policy import WorkflowRun


API_HOST = "api.github.com"
API_VERSION = "2026-03-10"
MAX_RESPONSE_BYTES = 256 * 1024
MAX_JSON_DEPTH = 12
SOCKET_TIMEOUT_SECONDS = 3
_ID = re.compile(r"[1-9][0-9]{0,19}")
_REPOSITORY = re.compile(r"[A-Za-z0-9-]{1,100}/[A-Za-z0-9_.-]{1,100}")
_PATH = re.compile(r"/repos/[A-Za-z0-9-]{1,100}/[A-Za-z0-9_.-]{1,100}/actions/runs/"
                   r"[1-9][0-9]{0,19}(?:/attempts/[1-9][0-9]{0,19})?")


class GitHubRunError(ValueError):
    """A sanitized failure to obtain metadata bound to the signed run identity."""


def _require(condition):
    if not condition:
        raise GitHubRunError("GitHub workflow run could not be verified")


def _matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _fetch_path(path: str, allowed) -> bytes:
    # only fixed host requests are possible; no proxy lookup, redirect handling or credentials
    _require(_matches(allowed, path) and "/../" not in path and "/./" not in path)
    connection = http.client.HTTPSConnection(API_HOST, timeout=SOCKET_TIMEOUT_SECONDS,
                                              context=ssl.create_default_context())
    deadline = time.monotonic() + SOCKET_TIMEOUT_SECONDS
    response = None
    try:
        connection.request("GET", path, headers={"Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION, "User-Agent": "rcp-production-admission"})
        transport = connection.sock
        response = connection.getresponse()
        _require(response.status == 200)
        _require(response.getheader("Content-Encoding", "identity") == "identity")
        remaining = MAX_RESPONSE_BYTES + 1
        chunks = []
        while remaining:
            timeout = deadline - time.monotonic()
            _require(timeout > 0)
            if transport is not None:
                transport.settimeout(timeout)
            chunk = response.read1(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        _require(remaining > 0 and time.monotonic() <= deadline)
        return b"".join(chunks)
    finally:
        if response is not None:
            response.close()
        connection.close()


def _fetch_official(path: str) -> bytes:
    return _fetch_path(path, _PATH)


_TERMINAL_PATH = re.compile(r"/repos/[A-Za-z0-9-]{1,100}/[A-Za-z0-9_.-]{1,100}/actions/runs/"
    r"[1-9][0-9]{0,19}(?:/attempts/[1-9][0-9]{0,19}(?:/jobs\?per_page=100)?|/pending_deployments)?")


def _fetch_terminal(path: str) -> bytes:
    return _fetch_path(path, _TERMINAL_PATH)


def _bounded_document(raw):
    _require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_RESPONSE_BYTES)
    value = parse_json(raw.decode("utf-8"))
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        _require(depth <= MAX_JSON_DEPTH)
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
    return value


def _document(raw):
    _require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_RESPONSE_BYTES)
    value = parse_json(raw.decode("utf-8"))
    _require(isinstance(value, dict))
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        _require(depth <= MAX_JSON_DEPTH)
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
    repository = value.get("repository")
    actor = value.get("actor")
    _require(isinstance(repository, dict) and isinstance(actor, dict))
    owner = repository.get("owner")
    _require(isinstance(owner, dict))
    # enforce the official integer schema before the policy's string identity comparison
    for identifier in (value.get("id"), value.get("run_attempt"), repository.get("id"),
                       owner.get("id"), actor.get("id")):
        _require(type(identifier) is int and 0 < identifier < 10**20)
    return value


class GitHubRunReader:
    """Fetch two public GitHub responses without credentials, caching or retries.

    Trust settings must come from the same server configuration as the JWT
    verifier. The optional injected fetcher accepts a generated API path and
    returns raw response bytes; it is trusted test/integration code. The clock
    returns Unix seconds and is sampled after both network reads finish.
    """

    def __init__(self, *, repository: str, repository_id: str, repository_owner_id: str,
                 fetcher: Callable[[str], bytes] | None = None,
                 clock: Callable[[], float] = time.time):
        _require(_matches(_REPOSITORY, repository) and repository.split("/")[1] not in {".", ".."})
        _require(_matches(_ID, repository_id) and _matches(_ID, repository_owner_id))
        self._repository = repository
        self._repository_id = repository_id
        self._owner_id = repository_owner_id
        self._fetch = fetcher if fetcher is not None else _fetch_official
        self._clock = clock

    def read(self, identity: VerifiedGitHubIdentity) -> WorkflowRun:
        """Return a producer-bound original creation proof or a sanitized error."""
        try:
            _require(isinstance(identity, VerifiedGitHubIdentity))
            _require(identity.repository == self._repository
                     and identity.repository_id == self._repository_id
                     and identity.repository_owner_id == self._owner_id
                     and identity.repository_owner == self._repository.split("/")[0])
            _require(_matches(_ID, identity.run_id) and _matches(_ID, identity.run_attempt))
            path = f"/repos/{self._repository}/actions/runs/{identity.run_id}"
            attempt = _document(self._fetch(path + "/attempts/" + identity.run_attempt))
            # read current run last: it must still identify this exact active attempt
            original = _document(self._fetch(path))
            seconds = self._clock()
            _require(type(seconds) in (int, float) and math.isfinite(seconds) and seconds > 0)
            now = datetime.fromtimestamp(seconds, timezone.utc)
            WorkflowRun.verify(attempt, identity, now)
            return WorkflowRun.verify(original, identity, now)
        except Exception:
            raise GitHubRunError("GitHub workflow run could not be verified") from None

class TerminalProducerReader(GitHubRunReader):
    """Read exact original/current terminal inventories for collection without a token.

    A current-attempt change during reads holds retirement. At most 100 jobs are
    supported; a larger/incomplete inventory fails rather than trusting one page.
    The collector independently validates every run/job against admitted claims.
    """
    def __init__(self, *, repository, repository_id, repository_owner_id, fetcher=None, clock=time.time):
        super().__init__(repository=repository, repository_id=repository_id,
            repository_owner_id=repository_owner_id, fetcher=fetcher or _fetch_terminal, clock=clock)

    def read(self, producer):
        try:
            _require(isinstance(producer, dict) and producer.get("repository") == self._repository
                and producer.get("repository_id") == self._repository_id
                and producer.get("repository_owner_id") == self._owner_id
                and _matches(_ID, producer.get("run_id")) and _matches(_ID, producer.get("run_attempt")))
            path = f"/repos/{self._repository}/actions/runs/{producer['run_id']}"
            original = _document(self._fetch(path + "/attempts/" + producer["run_attempt"]))
            current = _document(self._fetch(path))
            _require(current["run_attempt"] >= int(producer["run_attempt"]))
            def jobs(attempt, *, allow_empty=False):
                value = _bounded_document(self._fetch(path + "/attempts/" + str(attempt) + "/jobs?per_page=100"))
                _require(isinstance(value, dict) and set(value) == {"total_count", "jobs"}
                    and type(value["total_count"]) is int and (0 if allow_empty else 1) <= value["total_count"] <= 100
                    and isinstance(value["jobs"], list) and len(value["jobs"]) == value["total_count"])
                return value
            original_jobs = jobs(producer["run_attempt"])
            current_jobs = original_jobs if str(current["run_attempt"]) == producer["run_attempt"] else jobs(current["run_attempt"],
                allow_empty=current["status"] == "completed" and current["conclusion"] in {"cancelled", "startup_failure"})
            deployments = _bounded_document(self._fetch(path + "/pending_deployments"))
            _require(isinstance(deployments, list) and len(deployments) <= 100)
            after = _document(self._fetch(path))
            _require(all(after.get(key) == current.get(key) for key in
                ("id", "run_attempt", "head_sha", "status", "conclusion", "path", "event", "head_branch")))
            return dict(attempt=original, current=after, attempt_jobs=original_jobs,
                        current_jobs=current_jobs, pending_deployments=deployments)
        except Exception:
            raise GitHubRunError("GitHub terminal producer could not be verified") from None
