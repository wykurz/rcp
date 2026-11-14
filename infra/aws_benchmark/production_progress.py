"""Bounded public observations of controller-recorded progress, never final verdicts.

No extra service reads are required. These fields describe a consistent run-row
snapshot; only the retirement-bound report can establish a benchmark result.
"""

from datetime import timedelta
import re

from .common import durable_plan_contract, require, timestamp, utc
from .production_mailbox import MailboxPaths
from .production_objects import committed_status

SCHEMA = "rcp-production-recorded-progress-v1"
FIELDS = frozenset({"schema", "worker_outcome", "close_category", "capacity_deadline",
    "worker_deadline", "retirement_not_before", "instance_termination", "cleanup_stage",
    "cleanup_verified_at"})
CLOSE_CATEGORIES = {
    None: "none",
    "worker completed; result validity is separate from physical cleanup": "worker_finished",
    "allocated instance stopped or terminated": "instance_exited",
    "bound instance stopped or terminated": "instance_exited",
    "capacity wait expired": "capacity_exhausted",
    "immutable allocation runtime exhausted": "runtime_exhausted",
    "production kill switch is off": "disabled",
    "production input upload expired": "input_expired",
}
CLOSE_LABELS = {
    "none": "no closure recorded", "worker_finished": "worker finished",
    "instance_exited": "instance exited", "capacity_exhausted": "capacity wait expired",
    "runtime_exhausted": "allocation runtime expired", "disabled": "admission disabled",
    "input_expired": "input upload expired", "guard_problem": "safety guard problem",
    "other": "other recorded closure",
}


def date(value):
    """Return only a canonical UTC timestamp; never echo unvalidated source text."""
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        return timestamp(utc(value))
    except (ValueError, TypeError, OverflowError):
        return None


def worker_outcome(state, run_key):
    if state.get("worker_output") is None:
        return "unknown"
    try:
        request, binding = state["request"], state["binding"]
        require(isinstance(request, dict) and isinstance(binding, dict), "recorded worker context unavailable")
        run_id = re.fullmatch(r"repo-[1-9][0-9]*-run-([1-9][0-9]*)", run_key)
        require(run_id is not None and isinstance(request.get("run"), str) and
                re.fullmatch(re.escape(run_id[1]) + r"-[1-9][0-9]*", request["run"]),
                "recorded worker belongs to another run")
        paths = MailboxPaths.for_instance(request["pins"]["account"], request["pins"]["region"],
                                         binding["instance_id"])
        _, _, value = committed_status(state["worker_output"], paths.status)
        require(value.get("run") == request["run"] and value.get("state") == "completed"
                and type(value.get("success")) is bool, "invalid recorded worker outcome")
        if value["success"] or "sha256" in value:
            require(isinstance(value.get("sha256"), str) and
                    re.fullmatch(r"[0-9a-f]{64}", value["sha256"]), "invalid recorded archive digest")
        return "succeeded" if value["success"] else "failed"
    except (ValueError, TypeError, KeyError):
        # status is optional progress, not a way to publish malformed private data
        # or erase an independently validated retired report.
        return "unknown"


def observe(state, *, run_key):
    state = state if isinstance(state, dict) else {}
    request = state.get("request") if isinstance(state.get("request"), dict) else {}
    binding = state.get("binding") if isinstance(state.get("binding"), dict) else {}
    reason = state.get("close_reason")
    category = CLOSE_CATEGORIES.get(reason, "other") if isinstance(reason, (str, type(None))) else "other"
    if isinstance(reason, str) and reason.startswith("guard failure: "):
        category = "guard_problem"
    after = None
    try:
        outer = date(request.get("outer_deadline"))
        if outer is not None:
            after = timestamp(utc(outer) + timedelta(
                seconds=durable_plan_contract(request["plan"])["delivery_drain_seconds"]))
    except (ValueError, TypeError, KeyError, OverflowError):
        pass
    seen = state.get("seen", {})
    terminated = isinstance(seen, dict) and bool(seen) and all(
        isinstance(item, dict) and item.get("state") == "terminated" for item in seen.values())
    cleanup = state.get("cleanup")
    verified = date(cleanup.get("verified_at")) if isinstance(cleanup, dict) else None
    recorded = verified is not None and cleanup.get("live_instances") == [] and         cleanup.get("unresolved_allocation") is False
    value = dict(schema=SCHEMA, worker_outcome=worker_outcome(state, run_key), close_category=category,
        capacity_deadline=date(request.get("capacity_deadline")), worker_deadline=date(binding.get("deadline")),
        retirement_not_before=after, instance_termination="recorded" if terminated else "not_recorded",
        cleanup_stage="recorded" if recorded else "in_progress" if cleanup is not None else "not_recorded",
        cleanup_verified_at=verified if recorded else None)
    return validate(value)


def validate(value):
    require(isinstance(value, dict) and set(value) == FIELDS and value["schema"] == SCHEMA,
            "invalid recorded progress schema")
    require(all(isinstance(value[name], str) for name in
                ("worker_outcome", "close_category", "instance_termination", "cleanup_stage")),
            "invalid recorded progress category type")
    require(value["worker_outcome"] in {"unknown", "succeeded", "failed"} and
            value["close_category"] in CLOSE_LABELS and
            value["instance_termination"] in {"recorded", "not_recorded"} and
            value["cleanup_stage"] in {"recorded", "in_progress", "not_recorded"},
            "invalid recorded progress category")
    for name in ("capacity_deadline", "worker_deadline", "retirement_not_before", "cleanup_verified_at"):
        require(value[name] is None or date(value[name]) == value[name], "invalid recorded progress timestamp")
    require((value["cleanup_verified_at"] is not None) == (value["cleanup_stage"] == "recorded"),
            "recorded cleanup time differs")
    return value


def lines(value):
    """Fixed labels and canonical times are safe to escape in HTML/job summaries."""
    validate(value)
    worker = {"unknown": "not available", "succeeded": "success recorded", "failed": "failure recorded"}
    output = ["Recorded worker outcome: " + worker[value["worker_outcome"]] +
              " (provisional; final validation awaits retirement).",
              "Closure: " + CLOSE_LABELS[value["close_category"]] + "."]
    if value["instance_termination"] == "recorded":
        output.append("Instance termination is recorded by the controller; this is not a fresh EC2 check.")
    if value["cleanup_stage"] == "recorded":
        output.append("Controller cleanup verification recorded at " + value["cleanup_verified_at"] +
                      "; collection and ownership retirement remain pending.")
    else:
        output.append("Controller cleanup verification: " +
                      ("in progress." if value["cleanup_stage"] == "in_progress" else "not yet recorded."))
    for name, label in (("capacity_deadline", "Capacity search deadline"),
                        ("worker_deadline", "Worker useful-runtime deadline"),
                        ("retirement_not_before", "Earliest retirement safety time")):
        if value[name] is not None:
            output.append(label + ": " + value[name] + ".")
    return output
