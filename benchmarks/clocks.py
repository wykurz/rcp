"""Optional command-boundary clock observations; never a replacement timebase."""

import time


POLICY = "command-boundary-bracketed-v1"
SUPPORTED_POLICIES = frozenset({"command-boundary-bracketed-v1"})
QUALIFICATION = "Clock observations qualify timing only; elapsed_seconds and timeouts remain monotonic. RAW is not an external reference and durations are never rescaled. Bounds apply to sequential observations, not accuracy or exact command duration; scheduling can delay samples relative to command timestamps."
READINGS = ("monotonic_before", "raw", "realtime", "monotonic_after")
REASONS = frozenset({"unsupported", "read_failed"})
STATUSES = frozenset({"bounded", "no_completed_child", "monotonic_unavailable", "monotonic_regressed"})
_UNSUPPORTED, _READ_FAILED = object(), object()
_READ_ERRORS = (OSError, ValueError, OverflowError, NotImplementedError)
COMPARISON_KEYS = ("start_read_skew_ns", "finish_read_skew_ns", "monotonic_elapsed_bounds_ns",
                   "raw_elapsed_ns", "realtime_elapsed_ns", "raw_minus_monotonic_bounds_ns",
                   "realtime_minus_monotonic_bounds_ns")


def _reading(value):
    if value is _UNSUPPORTED:
        return dict(unavailable="unsupported")
    if value is _READ_FAILED:
        return dict(unavailable="read_failed")
    return dict(value_ns=value)


def sample():
    """Bracket sequential RAW and REALTIME reads with MONOTONIC reads."""
    monotonic = getattr(time, "monotonic_ns", None)
    raw_reader = getattr(time, "clock_gettime_ns", None)
    raw_id = getattr(time, "CLOCK_MONOTONIC_RAW", None)
    realtime_reader = getattr(time, "time_ns", None)
    before = raw = realtime = after = _UNSUPPORTED
    try:
        if monotonic is not None:
            before = monotonic()
    except InterruptedError:
        raise
    except _READ_ERRORS:
        before = _READ_FAILED
    try:
        if raw_reader is not None and raw_id is not None:
            raw = raw_reader(raw_id)
    except InterruptedError:
        raise
    except _READ_ERRORS:
        raw = _READ_FAILED
    try:
        if realtime_reader is not None:
            realtime = realtime_reader()
    except InterruptedError:
        raise
    except _READ_ERRORS:
        realtime = _READ_FAILED
    try:
        if monotonic is not None:
            after = monotonic()
    except InterruptedError:
        raise
    except _READ_ERRORS:
        after = _READ_FAILED
    return dict(monotonic_before=_reading(before), raw=_reading(raw), realtime=_reading(realtime), monotonic_after=_reading(after))


def unavailable_sample():
    """Record a failed boundary sample without retaining exception details."""
    return {clock: dict(unavailable="read_failed") for clock in READINGS}


def _value(sample, clock):
    return sample[clock].get("value_ns") if sample is not None else None


def _difference(finish, start):
    return finish - start if finish is not None and start is not None else None


def compare(start, finish):
    """Retain signed disagreements; only ordered monotonic brackets supply bounds."""
    before, after = (_value(start, key) for key in ("monotonic_before", "monotonic_after"))
    end_before, end_after = (_value(finish, key) for key in ("monotonic_before", "monotonic_after"))
    result = dict(status="bounded", start_read_skew_ns=_difference(after, before),
                  finish_read_skew_ns=_difference(end_after, end_before), monotonic_elapsed_bounds_ns=None)
    if finish is None:
        result["status"] = "no_completed_child"
    elif any(value is None for value in (before, after, end_before, end_after)):
        result["status"] = "monotonic_unavailable"
    elif not before <= after <= end_before <= end_after:
        result["status"] = "monotonic_regressed"
    else:
        result["monotonic_elapsed_bounds_ns"] = [end_before - after, end_after - before]
    for clock in ("raw", "realtime"):
        delta = _difference(_value(finish, clock), _value(start, clock))
        result[clock + "_elapsed_ns"] = delta
        bounds = result["monotonic_elapsed_bounds_ns"]
        result[clock + "_minus_monotonic_bounds_ns"] = [delta - bounds[1], delta - bounds[0]] if delta is not None and bounds is not None else None
    return result


def observation(start, finish, index):
    return dict(start=start, finish=finish, finish_command_index=index)


def comparison(value):
    """Derive current diagnostics from the stored primitive observations."""
    return compare(value["start"], value["finish"])


def _sample(value):
    if not isinstance(value, dict) or set(value) != set(READINGS):
        raise ValueError("invalid command clock sample")
    for reading in value.values():
        if not isinstance(reading, dict):
            raise ValueError("invalid command clock reading")
        if set(reading) == {"value_ns"}:
            number = reading["value_ns"]
            if type(number) is not int or not -(2**63) <= number < 2**63:
                raise ValueError("command clock nanoseconds must be signed 64-bit integers")
        elif set(reading) != {"unavailable"} or not isinstance(reading["unavailable"], str) or reading["unavailable"] not in REASONS:
            raise ValueError("invalid command clock availability")


def validate_observation(value, exit_codes):
    if not isinstance(exit_codes, list) or any(type(code) is not int for code in exit_codes):
        raise ValueError("command clocks require completed integer exit codes")
    if not isinstance(value, dict) or set(value) != {"start", "finish", "finish_command_index"}:
        raise ValueError("invalid command clock observation")
    _sample(value["start"])
    index = value["finish_command_index"]
    if value["finish"] is None:
        if index is not None or exit_codes:
            raise ValueError("command clocks omit a recorded child completion")
    else:
        _sample(value["finish"])
        if type(index) is not int or not 0 <= index < len(exit_codes):
            raise ValueError("invalid command clock completion index")


def validate(run):
    context = run["context"]
    enabled = "command_clocks" in context
    if enabled and (not isinstance(context["command_clocks"], str) or context["command_clocks"] not in SUPPORTED_POLICIES or context.get("topology") != "local"):
        raise ValueError("command clocks require the supported local policy")
    for row in run["trials"]:
        if "command_clocks" in row:
            if not enabled:
                raise ValueError("command clock observations require context policy")
            validate_observation(row["command_clocks"], row["exit_codes"])
        elif enabled and (row["status"] == "ok" or not isinstance(row["exit_codes"], list) or row["exit_codes"] or row.get("timed_out") is True or row.get("launch_error") is not None):
            raise ValueError("executed trial requires command clock observations")


def project(value):
    """Export only availability and derived seconds, withholding clock epochs."""
    observed = comparison(value)
    if not isinstance(observed["status"], str) or observed["status"] not in STATUSES:
        raise ValueError("invalid command clock comparison status")
    result = dict(finish_command_index=value["finish_command_index"], status=observed["status"],
                  epochs_withheld=True, availability={})
    for boundary in ("start", "finish"):
        record = value[boundary]
        if record is None:
            result["availability"][boundary] = None
            continue
        available = {}
        for clock in READINGS:
            reason = record[clock].get("unavailable", "available")
            if not isinstance(reason, str) or reason not in REASONS | {"available"}:
                raise ValueError("invalid command clock availability")
            available[clock] = reason
        result["availability"][boundary] = available
    for key in COMPARISON_KEYS:
        raw = observed[key]
        result[key.removesuffix("_ns") + "_seconds"] = [item / 1e9 for item in raw] if isinstance(raw, list) else raw / 1e9 if raw is not None else None
    return result
