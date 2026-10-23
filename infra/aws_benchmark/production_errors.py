"""Typed, sanitized failures that permit bounded production reconciliation."""

from .common import RevisionConflict, retryable_io


def transient(error):
    """Classify retryable failures without replaying writes or following arbitrary causes."""
    from .production_auth import GitHubIdentityTransientError
    from .production_github import GitHubRateLimitError, GitHubTransientError
    return isinstance(error, (RevisionConflict, GitHubIdentityTransientError,
                              GitHubRateLimitError, GitHubTransientError)) or retryable_io(error)
