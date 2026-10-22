"""Server-owned production admission policy; no caller can supply prices or allowances.

Money is integer micro-USD. Reservations accumulate until an explicitly reviewed
accounting operation reconciles them; successful work does not refund a reservation.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re

from .common import BINARIES, MAX_BINARY, require, timestamp, utc
from . import durable

MICRO_USD = 1_000_000
MAX_RUN_AGE_SECONDS = 7200
MAX_PRICE_AGE_SECONDS = 31 * 86400
SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"[0-9a-f]{64}")
PRODUCER_FIELDS = ("repository", "repository_id", "repository_owner_id", "ref", "workflow_ref",
                   "workflow_sha", "sha", "run_id", "run_attempt", "actor_id", "event_name")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def integer(value, minimum, maximum, message):
    require(type(value) is int and minimum <= value <= maximum, message)
    return value


@dataclass(frozen=True)
class Policy:
    version: str
    owner_id: str
    enabled: bool = False
    manual_actor_ids: tuple = ()
    schedule_slots: tuple = ()  # weekday (Monday=0), hour, minute in UTC
    scheduled_baseline: str = ""
    total_cap_microusd: int = 0
    monthly_cap_microusd: int = 0
    daily_run_limit: int = 0
    hourly_microusd: int = 0
    ancillary_microusd: int = 0
    priced_at: str = "1970-01-01T00:00:00+00:00"
    price_evidence: str = ""
    runtime_seconds: int = durable.RUNTIME_SECONDS
    capacity_seconds: int = durable.CAPACITY_SECONDS
    shutdown_seconds: int = durable.SHUTDOWN_ALLOWANCE_SECONDS
    schedule_delay_seconds: int = 1800

    def __post_init__(self):
        require(isinstance(self.version, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", self.version),
                "invalid production policy version")
        require(isinstance(self.owner_id, str) and re.fullmatch(r"[1-9][0-9]{0,19}", self.owner_id),
                "invalid immutable owner ID")
        require(type(self.enabled) is bool, "enabled must be a boolean")
        require(type(self.manual_actor_ids) is tuple and len(self.manual_actor_ids) <= 20
                and len(set(self.manual_actor_ids)) == len(self.manual_actor_ids)
                and all(isinstance(v, str) and re.fullmatch(r"[1-9][0-9]{0,19}", v)
                        for v in self.manual_actor_ids), "invalid explicit manual maintainer IDs")
        require(type(self.schedule_slots) is tuple and len(self.schedule_slots) <= 3,
                "at most three explicit weekly schedule slots are supported")
        for slot in self.schedule_slots:
            require(type(slot) is tuple and len(slot) == 3, "invalid schedule slot")
            integer(slot[0], 0, 6, "invalid schedule weekday")
            integer(slot[1], 0, 23, "invalid schedule hour")
            integer(slot[2], 0, 59, "invalid schedule minute")
        require(len(set(self.schedule_slots)) == len(self.schedule_slots), "duplicate schedule slots")
        require(not self.schedule_slots or isinstance(self.scheduled_baseline, str)
                and SHA.fullmatch(self.scheduled_baseline), "scheduled runs need an approved baseline SHA")
        for field in ("total_cap_microusd", "monthly_cap_microusd", "hourly_microusd", "ancillary_microusd"):
            integer(getattr(self, field), 0, 10**12, "invalid server money limit")
        integer(self.daily_run_limit, 0, 10, "invalid daily admission limit")
        integer(self.runtime_seconds, durable.minimum_runtime(), durable.RUNTIME_SECONDS,
                "runtime must fit the recorded workload plan")
        integer(self.capacity_seconds, 60, durable.CAPACITY_SECONDS, "capacity window exceeds 12 hours")
        integer(self.shutdown_seconds, durable.SHUTDOWN_ALLOWANCE_SECONDS, 3600,
                "shutdown allowance is below the reviewed margin")
        integer(self.schedule_delay_seconds, 1, 3600, "invalid schedule lateness allowance")
        require(isinstance(self.priced_at, str), "price timestamp must be a string")
        utc(self.priced_at)
        require(isinstance(self.price_evidence, str) and len(self.price_evidence.encode()) <= 2048,
                "invalid price evidence")

    def reservation(self, now):
        require(self.enabled, "production admission is disabled")
        require(0 <= (utc(now) - utc(self.priced_at)).total_seconds() <= MAX_PRICE_AGE_SECONDS,
                "server price evidence is stale")
        require(self.hourly_microusd > 0 and self.ancillary_microusd > 0 and self.price_evidence.strip(),
                "server price and ancillary allowances are not configured")
        require(self.total_cap_microusd > 0 and self.monthly_cap_microusd > 0 and self.daily_run_limit > 0,
                "server cost and run allowances are not configured")
        seconds = self.runtime_seconds + self.shutdown_seconds
        amount = (self.hourly_microusd * seconds + 3599) // 3600 + self.ancillary_microusd
        require(amount <= min(self.total_cap_microusd, self.monthly_cap_microusd),
                "full possible allocation does not fit policy allowances")
        return amount


def producer_identity(identity):
    return {key: getattr(identity, key) for key in PRODUCER_FIELDS}


def producer_fingerprint(identity):
    return hashlib.sha256(canonical(producer_identity(identity))).hexdigest()


@dataclass(frozen=True)
class WorkflowRun:
    created_at: str
    observed_at: str
    producer_fingerprint: str

    @classmethod
    def verify(cls, value, identity, now):
        require(isinstance(value, dict), "invalid GitHub workflow run")
        repository = value.get("repository", {})
        owner = repository.get("owner", {})
        require(str(value.get("id")) == identity.run_id
                and str(value.get("run_attempt")) == identity.run_attempt
                and str(repository.get("id")) == identity.repository_id
                and repository.get("full_name") == identity.repository
                and str(owner.get("id")) == identity.repository_owner_id,
                "GitHub run identity differs from signed claims")
        require(value.get("head_sha") == identity.sha and value.get("head_branch") == "main"
                and value.get("event") == identity.event_name
                and str(value.get("actor", {}).get("id")) == identity.actor_id,
                "GitHub run producer differs from signed claims")
        path = identity.workflow_ref[len(identity.repository) + 1:].split("@", 1)[0]
        require(value.get("path") == path and value.get("status") in ("queued", "in_progress")
                and value.get("conclusion") is None, "GitHub run is not an active approved workflow")
        created = value.get("created_at")
        require(isinstance(created, str) and 0 <= (utc(now) - utc(created)).total_seconds() <= MAX_RUN_AGE_SECONDS,
                "workflow run is stale or created in the future")
        return cls(created_at=timestamp(created), observed_at=timestamp(now),
                   producer_fingerprint=producer_fingerprint(identity))


def manifest(value):
    require(isinstance(value, dict) and set(value) == {"baseline", "candidate", "suite", "binaries"},
            "unexpected benchmark manifest fields")
    require(all(isinstance(value[side], str) and SHA.fullmatch(value[side]) for side in ("baseline", "candidate")),
            "benchmark revisions must be pinned full SHAs")
    require(value["suite"] == "bulk-1g-metadata", "production suite is not allowlisted")
    # The first unattended acceptance uses the measured A/A workload. A/B admission
    # needs its own reviewed contract and must not reinterpret historical A/A proof.
    require(value["baseline"] == value["candidate"], "initial production acceptance requires A/A")
    binaries = value["binaries"]
    require(isinstance(binaries, dict) and set(binaries) == set(BINARIES), "unexpected binary manifest")
    for item in binaries.values():
        require(isinstance(item, dict) and set(item) == {"sha256", "bytes"}
                and isinstance(item["sha256"], str) and DIGEST.fullmatch(item["sha256"]),
                "invalid binary manifest entry")
        integer(item["bytes"], 1, MAX_BINARY, "binary exceeds size bound")
    require(all(binaries["baseline/" + name] == binaries["candidate/" + name]
                for name in ("rcp", "rcpd", "filegen")), "A/A executable bytes differ")
    return json.loads(canonical(value))


def schedule_slot(policy, created, now):
    created, now = utc(created), utc(now)
    candidates = []
    for weekday, hour, minute in policy.schedule_slots:
        day = created.date() - timedelta(days=(created.weekday() - weekday) % 7)
        slot = datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)
        if 0 <= (created - slot).total_seconds() <= policy.schedule_delay_seconds:
            candidates.append(slot)
    require(len(candidates) == 1, "no unique approved schedule slot matches this run")
    slot = candidates[0]
    require(0 <= (now - slot).total_seconds() <= policy.schedule_delay_seconds + MAX_RUN_AGE_SECONDS,
            "approved schedule slot expired")
    return slot.strftime("%Y-%m-%dT%H:%MZ")


def authorize(policy, identity, run, value, now):
    """Derive authorization solely from verified claims/run metadata and server policy."""
    reserve = policy.reservation(now)
    require(isinstance(run, WorkflowRun) and run.producer_fingerprint == producer_fingerprint(identity)
            and 0 <= (utc(now) - utc(run.observed_at)).total_seconds() <= 60
            and 0 <= (utc(now) - utc(run.created_at)).total_seconds() <= MAX_RUN_AGE_SECONDS,
            "workflow run observation is stale")
    require(identity.repository_owner_id == policy.owner_id, "policy owner differs from repository owner")
    checked = manifest(value)
    slot = None
    if identity.event_name == "workflow_dispatch":
        require(identity.actor_id in (policy.owner_id, *policy.manual_actor_ids), "manual requester is not approved")
    elif identity.event_name == "schedule":
        slot = schedule_slot(policy, run.created_at, now)
        require(checked["baseline"] == policy.scheduled_baseline and checked["candidate"] == identity.sha,
                "scheduled revisions differ from approved baseline and main")
    else:
        raise ValueError("only approved manual and scheduled requests may enter admission")
    producer = producer_identity(identity)
    producer["created_at"] = run.created_at
    run_key = "repo-" + identity.repository_id + "-run-" + identity.run_id
    epoch = hashlib.sha256(run_key.encode()).hexdigest()
    intent = dict(producer=producer, manifest=checked, policy_version=policy.version)
    return dict(run_key=run_key, epoch=epoch, fingerprint=hashlib.sha256(canonical(intent)).hexdigest(),
        producer=producer, manifest=checked, reserve_microusd=reserve, schedule_slot=slot,
        month=utc(now).strftime("%Y-%m"), day=utc(now).strftime("%Y-%m-%d"))
