"""Optional command-boundary clock observations; never a replacement timebase."""

import time


POLICY = "command-boundary-bracketed-v1"
QUALIFICATION = "Clock observations qualify timing only; elapsed_seconds and timeouts remain monotonic. RAW is not an external reference and durations are never rescaled. Bounds apply to sequential observations, not accuracy or exact command duration; scheduling can delay samples relative to command timestamps."
READINGS = ("monotonic_before", "raw", "realtime", "monotonic_after")
REASONS = frozenset({"unsupported", "read_failed"})
COMPARISON_KEYS = ("start_read_skew_ns", "finish_read_skew_ns", "monotonic_elapsed_bounds_ns",
                   "raw_elapsed_ns", "realtime_elapsed_ns", "raw_minus_monotonic_bounds_ns",
                   "realtime_minus_monotonic_bounds_ns")


def _read(function, *args):
    if function is None:
        return dict(unavailable="unsupported")
    try:
        return dict(value_ns=function(*args))
    except (OSError, ValueError, OverflowError, NotImplementedError):
        return dict(unavailable="read_failed")


def sample():
    """Bracket sequential RAW and REALTIME reads with MONOTONIC reads."""
    monotonic = getattr(time, "monotonic_ns", None)
    before = _read(monotonic)
    raw_id = getattr(time, "CLOCK_MONOTONIC_RAW", None)
    raw = _read(getattr(time, "clock_gettime_ns", None), raw_id) if raw_id is not None else dict(unavailable="unsupported")
    realtime = _read(getattr(time, "time_ns", None))
    after = _read(monotonic)
    return dict(monotonic_before=before, raw=raw, realtime=realtime, monotonic_after=after)


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
    elif after < before or end_after < end_before or end_after < before:
        result["status"] = "monotonic_regressed"
    elif end_before < after:
        result["status"] = "overlapping_brackets"
    else:
        result["monotonic_elapsed_bounds_ns"] = [end_before - after, end_after - before]
    for clock in ("raw", "realtime"):
        delta = _difference(_value(finish, clock), _value(start, clock))
        result[clock + "_elapsed_ns"] = delta
        bounds = result["monotonic_elapsed_bounds_ns"]
        result[clock + "_minus_monotonic_bounds_ns"] = [delta - bounds[1], delta - bounds[0]] if delta is not None and bounds is not None else None
    return result


def observation(start, finish, index):
    return dict(start=start, finish=finish, finish_command_index=index, comparison=compare(start, finish))


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


def _exact(actual, expected):
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(_exact(actual[key], item) for key, item in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(_exact(a, b) for a, b in zip(actual, expected))
    return actual == expected


def validate_observation(value, exit_codes):
    if not isinstance(exit_codes, list) or any(type(code) is not int for code in exit_codes):
        raise ValueError("command clocks require completed integer exit codes")
    if not isinstance(value, dict) or set(value) != {"start", "finish", "finish_command_index", "comparison"}:
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
    if not _exact(value["comparison"], compare(value["start"], value["finish"])):
        raise ValueError("command clock comparison differs from its observations")


def validate(run):
    context = run["context"]
    enabled = "command_clocks" in context
    if enabled and (context["command_clocks"] != POLICY or context.get("topology") != "local"):
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
    result = dict(finish_command_index=value["finish_command_index"], status=value["comparison"]["status"],
                  epochs_withheld=True, availability={})
    for boundary in ("start", "finish"):
        record = value[boundary]
        result["availability"][boundary] = {clock: record[clock].get("unavailable", "available") for clock in READINGS} if record is not None else None
    for key in COMPARISON_KEYS:
        raw = value["comparison"][key]
        result[key.removesuffix("_ns") + "_seconds"] = [item / 1e9 for item in raw] if isinstance(raw, list) else raw / 1e9 if raw is not None else None
    return result
