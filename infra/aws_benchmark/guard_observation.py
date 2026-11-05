"""Classify a consistent janitor read without retaining untrusted payloads.

This pure observation is reusable by coordinators and local operators. A future
scan never grants freshness: only a subsequent read at a sampled clock can do so.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
import re

from . import durable
from .common import require, utc

GUARD_READ_ATTEMPTS = 3


class GuardStatus(Enum):
    FRESH = "fresh"
    MISSING = "missing"
    INVALID = "invalid"
    FUTURE = "future"
    STALE = "stale"


@dataclass(frozen=True)
class GuardObservation:
    status: GuardStatus
    observed_at: str | None = None
    age_seconds: float | None = None

    def diagnostic(self, *, checked_at: datetime, attempt: int, request_id=None) -> dict:
        """Return bounded JSON fields without raw payloads or parser diagnostics."""
        require(type(attempt) is int and 1 <= attempt <= GUARD_READ_ATTEMPTS,
                "invalid guard diagnostic attempt")
        try:
            sampled_at = utc(checked_at).isoformat(timespec="microseconds")
        except (TypeError, ValueError, AttributeError, OverflowError):
            sampled_at = None
        detail = dict(event="guard_observation", status=self.status.value, observed_at=self.observed_at,
                      age_seconds=self.age_seconds, checked_at=sampled_at, attempt=attempt)
        if isinstance(request_id, str) and re.fullmatch(r"[A-Za-z0-9-]{1,64}", request_id):
            detail["request_id"] = request_id
        return detail


def _unique_object(pairs):
    value = {}
    for key, member in pairs:
        if key in value:
            raise ValueError("duplicate guard field")
        value[key] = member
    return value


def _invalid_constant(value):
    raise ValueError("nonfinite guard value")


def classify_guard(response, *, namespace: str, now: datetime) -> GuardObservation:
    """Validate a DynamoDB guard response at a clock sampled after its read.

    Diagnostics retain only a status, a validated timestamp and its signed age.
    They never retain raw JSON, foreign namespace values or parser exceptions.
    """
    if not isinstance(response, dict):
        return GuardObservation(GuardStatus.INVALID)
    item = response.get("Item")
    if item is None:
        return GuardObservation(GuardStatus.MISSING)
    try:
        raw = item["payload"]["S"]
        if not isinstance(raw, str):
            return GuardObservation(GuardStatus.INVALID)
        proof = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        if not isinstance(proof, dict) or proof.get("namespace") != namespace or proof.get("healthy") is not True:
            return GuardObservation(GuardStatus.INVALID)
        observed_at = proof["observed_at"]
        if (not isinstance(observed_at, str) or len(observed_at) > 64 or
                not all(32 <= ord(character) < 127 for character in observed_at)):
            return GuardObservation(GuardStatus.INVALID)
        age = (utc(now) - utc(observed_at)).total_seconds()
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError, RecursionError):
        return GuardObservation(GuardStatus.INVALID)
    status = (GuardStatus.FUTURE if age < 0 else
              GuardStatus.STALE if age > durable.GUARD_GAP_SECONDS else GuardStatus.FRESH)
    return GuardObservation(status, observed_at, age)
