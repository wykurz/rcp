"""Read and bind authenticated GitHub run metadata to a verified admission identity.

The attempt endpoint's created_at is the attempt creation time on reruns. The
run endpoint preserves the original creation time, so both responses are checked
and only the run endpoint supplies the admission age proof.

REST contract: https://docs.github.com/en/rest/actions/workflow-runs
Production readers require an authenticated observation factory. Each observation
shares one bounded secret/token/GET budget and permits one refresh. Test fetchers
must be supplied explicitly; there is no anonymous network fallback. Failures do
not yield an age proof, and producer observations are never cached.
"""

from collections.abc import Callable
from contextlib import nullcontext
from datetime import datetime, timezone
import math
import re
import time

from benchmarks.strict_json import parse_json
from .production_auth import VerifiedGitHubIdentity, _retryable_transport
from .production_policy import WorkflowRun


API_HOST = "api.github.com"
API_VERSION = "2026-03-10"
MAX_RESPONSE_BYTES = 256 * 1024
MAX_JSON_DEPTH = 12
_ID = re.compile(r"[1-9][0-9]{0,19}")
_REPOSITORY = re.compile(r"[A-Za-z0-9-]{1,100}/[A-Za-z0-9_.-]{1,100}")
class GitHubRunError(ValueError):
    """A sanitized failure to obtain metadata bound to the signed run identity."""


class GitHubTransientError(GitHubRunError):
    """A sanitized observation outage; retries must obtain fresh complete proofs."""

    def __init__(self, retry_after_seconds=5):
        super().__init__("GitHub observation temporarily unavailable")
        self.retry_after_seconds = retry_after_seconds


class GitHubRateLimitError(GitHubRunError):
    """A sanitized retry delay, never a substitute for fresh producer metadata."""

    def __init__(self, retry_after_seconds):
        super().__init__("GitHub API rate limit; producer verification remains pending")
        self.retry_after_seconds = retry_after_seconds


def _require(condition):
    if not condition:
        raise GitHubRunError("GitHub workflow run could not be verified")


def _matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


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
    value = _bounded_document(raw)
    _require(isinstance(value, dict))
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
    """Bind fresh responses from one authenticated observation to signed claims.

    Trust settings match the JWT verifier. Production supplies observation_factory;
    explicit raw fetcher injection is reserved for trusted tests/integration.
    The clock is sampled after both network reads finish.
    """

    def __init__(self, *, repository: str, repository_id: str, repository_owner_id: str,
                 fetcher: Callable[[str], bytes] | None = None, observation_factory=None,
                 clock: Callable[[], float] = time.time):
        _require(_matches(_REPOSITORY, repository) and repository.split("/")[1] not in {".", ".."})
        _require(_matches(_ID, repository_id) and _matches(_ID, repository_owner_id))
        self._repository = repository
        self._repository_id = repository_id
        self._owner_id = repository_owner_id
        _require((fetcher is None) != (observation_factory is None))
        _require(callable(fetcher) if fetcher is not None else callable(observation_factory))
        self._observation = observation_factory if observation_factory is not None else lambda: nullcontext(fetcher)
        self._clock = clock

    def read(self, identity: VerifiedGitHubIdentity) -> WorkflowRun:
        """Return a producer-bound original creation proof or a sanitized error."""
        try:
            with self._observation() as fetch:
                _require(isinstance(identity, VerifiedGitHubIdentity))
                _require(identity.repository == self._repository
                         and identity.repository_id == self._repository_id
                         and identity.repository_owner_id == self._owner_id
                         and identity.repository_owner == self._repository.split("/")[0])
                _require(_matches(_ID, identity.run_id) and _matches(_ID, identity.run_attempt))
                path = f"/repos/{self._repository}/actions/runs/{identity.run_id}"
                attempt = _document(fetch(path + "/attempts/" + identity.run_attempt))
                # read current run last: it must still identify this exact active attempt
                original = _document(fetch(path))
                seconds = self._clock()
                _require(type(seconds) in (int, float) and math.isfinite(seconds) and seconds > 0)
                now = datetime.fromtimestamp(seconds, timezone.utc)
                WorkflowRun.verify(attempt, identity, now)
                return WorkflowRun.verify(original, identity, now)
        except (GitHubRateLimitError, GitHubTransientError):
            raise
        except Exception as error:
            if _retryable_transport(error):
                raise GitHubTransientError() from None
            raise GitHubRunError("GitHub workflow run could not be verified") from None

class TerminalProducerReader(GitHubRunReader):
    """Read exact original/current terminal inventories in one authenticated observation.

    A current-attempt change during reads holds retirement. At most 100 jobs are
    supported; a larger/incomplete inventory fails rather than trusting one page.
    The collector independently validates every run/job against admitted claims.
    """
    def __init__(self, *, repository, repository_id, repository_owner_id, fetcher=None,
                 observation_factory=None, clock=time.time):
        super().__init__(repository=repository, repository_id=repository_id,
            repository_owner_id=repository_owner_id, fetcher=fetcher,
            observation_factory=observation_factory, clock=clock)

    def read(self, producer):
        try:
            with self._observation() as fetch:
                _require(isinstance(producer, dict) and producer.get("repository") == self._repository
                    and producer.get("repository_id") == self._repository_id
                    and producer.get("repository_owner_id") == self._owner_id
                    and _matches(_ID, producer.get("run_id")) and _matches(_ID, producer.get("run_attempt")))
                path = f"/repos/{self._repository}/actions/runs/{producer['run_id']}"
                original = _document(fetch(path + "/attempts/" + producer["run_attempt"]))
                current = _document(fetch(path))
                _require(current["run_attempt"] >= int(producer["run_attempt"]))
                def jobs(attempt, *, allow_empty=False):
                    value = _bounded_document(fetch(path + "/attempts/" + str(attempt) + "/jobs?per_page=100"))
                    _require(isinstance(value, dict) and set(value) == {"total_count", "jobs"}
                        and type(value["total_count"]) is int and (0 if allow_empty else 1) <= value["total_count"] <= 100
                        and isinstance(value["jobs"], list) and len(value["jobs"]) == value["total_count"])
                    return value
                original_jobs = jobs(producer["run_attempt"])
                current_jobs = original_jobs if str(current["run_attempt"]) == producer["run_attempt"] else jobs(current["run_attempt"],
                    allow_empty=current["status"] == "completed" and current["conclusion"] in {"cancelled", "startup_failure"})
                deployments = _bounded_document(fetch(path + "/pending_deployments"))
                _require(isinstance(deployments, list) and len(deployments) <= 100)
                after = _document(fetch(path))
                _require(all(after.get(key) == current.get(key) for key in
                    ("id", "run_attempt", "head_sha", "status", "conclusion", "path", "event", "head_branch")))
                return dict(attempt=original, current=after, attempt_jobs=original_jobs,
                            current_jobs=current_jobs, pending_deployments=deployments)
        except (GitHubRateLimitError, GitHubTransientError):
            raise
        except Exception as error:
            if _retryable_transport(error):
                raise GitHubTransientError() from None
            raise GitHubRunError("GitHub terminal producer could not be verified") from None
