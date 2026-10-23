"""Versioned direct-EC2 admission contract; this module performs no I/O.

Capacity rejection is free of instance allocation; an ambiguous launch consumes the
attempt. Every possible allocation keeps its original deadline, even after a timeout.
"""

from datetime import timedelta
from decimal import Decimal
import hashlib
import json
import re

from .common import (DURABLE_PLAN, DURABLE_MAX_RUNTIME, DURABLE_MAX_PROVISIONING,
                     durable_worker_reserve, durable_plan_contract, durable_minimum_runtime,
                     validate_durable_hashes, validate_durable_pins, DIRECT_PROTOCOL, DIRECT_MAX_ATTEMPTS,
                     INSTANCE_TYPE, require, timestamp, utc, validate_attempt_id, validate_identity)

PROTOCOL = DIRECT_PROTOCOL
CAPACITY_SECONDS = DURABLE_MAX_PROVISIONING
RUNTIME_SECONDS = DURABLE_MAX_RUNTIME
GUARD_GAP_SECONDS = 120
LAUNCH_ACK_SECONDS = 90
DELIVERY_DRAIN_SECONDS = durable_plan_contract(DURABLE_PLAN)["delivery_drain_seconds"]
SHUTDOWN_ALLOWANCE_SECONDS = 16 * 60
RETRY_SECONDS = 60
MAX_ATTEMPTS = DIRECT_MAX_ATTEMPTS
PLAN = DURABLE_PLAN
worker_minimum = durable_worker_reserve
TAG_NAMESPACE = "rcp:namespace"
TAG_RUN = "rcp:run"
TAG_ATTEMPT = "rcp:attempt"
TAG_DEADLINE = "rcp:deadline"
TAG_CREATED = "rcp:created-at"
TAG_KEYS = {TAG_NAMESPACE, TAG_RUN, TAG_ATTEMPT, TAG_DEADLINE, TAG_CREATED}
PIN_KEYS = frozenset(durable_plan_contract(PLAN)["pin_patterns"])
minimum_runtime = durable_minimum_runtime


def validate(value, *, exact=True):
    require(isinstance(value, dict) and set(value) == {
        "protocol", "run", "baseline", "candidate", "suite", "controller", "created_at",
        "capacity_deadline", "outer_deadline", "runtime_seconds", "plan", "sha256", "pins"},
        "unexpected direct request fields")
    require(value["protocol"] == PROTOCOL, "unknown direct capacity protocol")
    validate_identity(value)
    require(value["suite"] == "bulk-1g-metadata", "direct qualification requires metadata suite")
    require(value["baseline"] == value["candidate"], "direct qualification requires A/A revisions")
    require(re.fullmatch(r"[0-9a-f]{40}", value["controller"]) is not None, "invalid controller revision")
    contract = durable_plan_contract(value["plan"], exact=exact)
    seconds = value["runtime_seconds"]
    require(type(seconds) is int and minimum_runtime(value["plan"], exact=exact) <= seconds <= contract["max_runtime"],
            "runtime does not fit the explicit workload/headroom plan or exceeds 180 minutes")
    created, expiry, outer = (utc(value[k]) for k in ("created_at", "capacity_deadline", "outer_deadline"))
    require(0 < (expiry - created).total_seconds() <= contract["max_provisioning"], "capacity wait exceeds the recorded policy")
    require(outer == expiry + timedelta(seconds=seconds), "control reconciliation horizon differs")
    validate_durable_hashes(value["sha256"], value["plan"], exact=exact)
    validate_durable_pins(value["pins"], value["plan"], exact=exact)
    return value


def request(identity, created, hashes, pins, *, runtime_seconds=RUNTIME_SECONDS, capacity_seconds=CAPACITY_SECONDS):
    require(type(capacity_seconds) is int, "capacity seconds must be an integer")
    expiry = utc(created) + timedelta(seconds=capacity_seconds)
    value = {k: identity[k] for k in ("run", "baseline", "candidate", "suite", "controller")}
    value.update(protocol=PROTOCOL, created_at=timestamp(created), capacity_deadline=timestamp(expiry),
                 outer_deadline=timestamp(expiry + timedelta(seconds=runtime_seconds)), runtime_seconds=runtime_seconds,
                 plan=dict(PLAN), sha256=dict(hashes), pins=dict(pins))
    return validate(value)


def initial_lease(value):
    validate(value)
    return dict(request=value, phase="armed", execution_arn=None, attempt=None,
                attempts=0, capacity_failures=[], next_attempt_at=value["created_at"], binding=None,
                known_instance_ids=[], seen={}, close_reason=None, allocation_consumed=False,
                runtime_verified=False, worker_published=False, cleanup=None, launch_outcome="none")


def attempt(value, ordinal, now):
    validate(value)
    require(type(ordinal) is int and 1 <= ordinal <= MAX_ATTEMPTS, "capacity attempt bound exhausted")
    require(utc(value["created_at"]) <= now < utc(value["capacity_deadline"]), "capacity request expired")
    result = dict(id=value["run"] + "-" + str(ordinal), ordinal=ordinal, started_at=timestamp(now),
                  close_at=timestamp(now + timedelta(seconds=value["runtime_seconds"])), parameters={})
    material = json.dumps([value, result["id"], result["started_at"], result["close_at"]], sort_keys=True).encode()
    result["token"] = hashlib.sha256(material).hexdigest()
    return result


def tags(value, allocation):
    return {TAG_NAMESPACE: value["pins"]["namespace"], TAG_RUN: value["run"], TAG_ATTEMPT: allocation["id"],
            TAG_DEADLINE: allocation["close_at"], TAG_CREATED: allocation["started_at"]}


def instance_tags(instance):
    values = instance.get("Tags", [])
    require(isinstance(values, list) and all(isinstance(t, dict) and set(t) == {"Key", "Value"} for t in values),
            "malformed instance tags")
    require(len({t["Key"] for t in values}) == len(values), "duplicate instance tags")
    return {t["Key"]: t["Value"] for t in values}


def tagged_expiry(instance, namespace, now):
    """The janitor can enforce these bounds without a lease, execution or coordinator."""
    values = instance_tags(instance)
    require(values.get(TAG_NAMESPACE) == namespace, "instance is outside managed namespace")
    require(TAG_KEYS <= set(values), "missing allocation deadline tags")
    require(re.fullmatch(r"[1-9][0-9]{0,19}-[1-9][0-9]{0,4}", values[TAG_RUN]), "invalid run tag")
    validate_attempt_id(values[TAG_RUN], values[TAG_ATTEMPT])
    start, close, launch = utc(values[TAG_CREATED]), utc(values[TAG_DEADLINE]), utc(instance["LaunchTime"])
    require(timestamp(start) == values[TAG_CREATED] and timestamp(close) == values[TAG_DEADLINE],
            "deadline tags must be canonical UTC")
    require(start <= launch <= now and 0 < (close - start).total_seconds() <= RUNTIME_SECONDS
            and close <= launch + timedelta(seconds=RUNTIME_SECONDS), "excessive or inconsistent tagged deadline")
    return close


def validate_allocation_instance(value, instance, allocation):
    """Identify accepted allocation evidence without authorizing useful worker runtime."""
    validate(value)
    start, launch = utc(allocation["started_at"]), utc(instance["LaunchTime"])
    require(utc(value["created_at"]) <= start < utc(value["capacity_deadline"]) and start <= launch,
            "instance launch is outside this allocation interval")
    require(re.fullmatch(r"i-[0-9a-f]{17}", instance.get("InstanceId", "")), "invalid allocated instance identity")
    require(allocation["close_at"] == timestamp(start + timedelta(seconds=value["runtime_seconds"])),
            "allocation deadline changed")
    require(instance.get("ClientToken") == allocation["token"], "instance token differs from allocation")
    pins = value["pins"]
    require(instance["ImageId"] == pins["ami"] and instance.get("InstanceType") == INSTANCE_TYPE
            and not instance.get("InstanceLifecycle"), "instance image, type or purchase path differs")
    terminal = instance.get("State", {}).get("Name") in ("shutting-down", "terminated")
    require((terminal and "SubnetId" not in instance or instance.get("SubnetId") == pins["subnet"]) and
            (terminal and "IamInstanceProfile" not in instance or
             instance.get("IamInstanceProfile", {}).get("Arn") == pins["profile_arn"]) and
            (terminal and not instance.get("SecurityGroups") or
             {g["GroupId"] for g in instance.get("SecurityGroups", [])} == {pins["security_group"]}),
            "instance network or profile differs")
    observed = instance_tags(instance)
    require(all(observed.get(k) == v for k, v in tags(value, allocation).items()), "allocation tags differ")
    return launch


def bind(value, instance, allocation):
    launch = validate_allocation_instance(value, instance, allocation)
    require(launch < utc(value["capacity_deadline"]), "instance launch is outside this allocation interval")
    require(instance.get("State", {}).get("Name") in ("pending", "running"), "instance is not eligible for runtime")
    return dict(instance_id=instance["InstanceId"], image_id=instance["ImageId"],
                instance_type=instance["InstanceType"], launch_at=timestamp(launch),
                close_at=allocation["close_at"], deadline=timestamp(utc(allocation["close_at"]) - timedelta(
                    seconds=value["plan"]["shutdown_reserve_seconds"])), attempt_id=allocation["id"],
                attempt_started_at=allocation["started_at"])


def window(value, binding, *, exact=True):
    validate(value, exact=exact)
    result = {k: value[k] for k in ("protocol", "run", "baseline", "candidate", "suite", "runtime_seconds", "plan", "outer_deadline")}
    result.update(instance_id=binding["instance_id"], launch_at=binding["launch_at"],
                  opens=binding["attempt_started_at"], deadline=binding["deadline"],
                  request_expires=value["capacity_deadline"], attempt_id=binding["attempt_id"],
                  attempt_started_at=binding["attempt_started_at"])
    return result


def worker_command(value, binding, *, exact=True):
    validate(value, exact=exact)
    command = {key: value[key] for key in ("run", "baseline", "candidate", "suite", "sha256")}
    return dict(command, deadline=binding["deadline"])


def unresolved_launch(state):
    """One unobserved immutable attempt can be replayed, never replaced by a new one."""
    return (state.get("phase") in {"launching", "uncertain"} and state.get("launch_outcome") == "uncertain" and
            state.get("allocation_consumed") is True and isinstance(state.get("attempt"), dict) and
            state.get("binding") is None and state.get("known_instance_ids") == [] and
            state.get("seen") == {} and state.get("close_reason") is None)


def launch_sender(state):
    """Identify a potential sender; elapsed time never proves an earlier send failed."""
    require("launch_replayed" not in state, "obsolete launch sender marker is unsupported")
    attempt = state.get("attempt")
    require(isinstance(attempt, dict), "launch sender has no attempt")
    value = state.get("launch_sender", dict(sequence=1, at=attempt["started_at"]))
    require(isinstance(value, dict) and set(value) == {"sequence", "at"} and
            type(value["sequence"]) is int and 1 <= value["sequence"] <= 2 ** 63 - 1 and
            isinstance(value["at"], str) and timestamp(utc(value["at"])) == value["at"] and
            utc(attempt["started_at"]) <= utc(value["at"]) < utc(attempt["close_at"]),
            "invalid launch sender sequence or timestamp")
    return value


def launch_outcome(state, outcome):
    """Keep accepted evidence monotonic and the compatibility receipt flag consistent."""
    require(outcome in {"none", "not-sent", "rejected", "capacity-rejected", "uncertain", "accepted"}, "unknown launch outcome")
    require(state["launch_outcome"] != "accepted" or outcome == "accepted", "accepted allocation cannot be downgraded")
    consumed = outcome in {"uncertain", "accepted"}
    require(consumed or not (state["known_instance_ids"] or state["seen"]),
            "no-allocation evidence conflicts with observed instances")
    state.update(launch_outcome=outcome, allocation_consumed=consumed)


def cost_projection(*, hourly, ancillary, runtime_seconds=RUNTIME_SECONDS,
                    capacity_seconds=CAPACITY_SECONDS, shutdown_seconds=SHUTDOWN_ALLOWANCE_SECONDS):
    require(type(runtime_seconds) is int and minimum_runtime() <= runtime_seconds <= RUNTIME_SECONDS,
            "invalid priced runtime")
    require(type(capacity_seconds) is int and 0 < capacity_seconds <= CAPACITY_SECONDS, "invalid priced wait")
    require(type(shutdown_seconds) is int and shutdown_seconds >= SHUTDOWN_ALLOWANCE_SECONDS,
            "at least sixteen minutes of shutdown planning allowance is required")
    rate, other = Decimal(str(hourly)), Decimal(str(ancillary))
    require(rate.is_finite() and other.is_finite() and rate > 0 and other >= 0, "invalid prices")
    cost = rate * Decimal(runtime_seconds + shutdown_seconds) / Decimal(3600) + other
    return dict(normal_usd=str(cost), guarded_failure_usd=str(cost), runtime_seconds=runtime_seconds,
                capacity_seconds=capacity_seconds, shutdown_allowance_seconds=shutdown_seconds,
                note="One possible allocation. Planning estimate, not a billing cap; simultaneous failure of independent termination paths can exceed it.")
