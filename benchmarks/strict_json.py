"""Parse JSON without duplicate keys or nonfinite numbers."""

import json
import math


def _reject_constant(value):
    raise ValueError(f"invalid JSON number: {value}")


def _finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"nonfinite JSON number: {value}")
    return number


def _unique_pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def parse_json(text):
    """Parse JSON, rejecting ambiguous keys and nonfinite numeric values."""
    return json.loads(text, parse_constant=_reject_constant, parse_float=_finite_float, object_pairs_hook=_unique_pairs)
