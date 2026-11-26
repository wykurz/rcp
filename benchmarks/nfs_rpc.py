"""Bounded NFSv4.2 client RPC counters for one mount over a command window."""

from dataclasses import dataclass
import os
from pathlib import Path
import re


READ_BYTES = 512 * 1024
MAX_NUMBER = (1 << 63) - 1
OPERATIONS = ("LOOKUP", "GETATTR", "READDIR", "LINK")
COUNTERS = ("completed", "transmissions", "retransmissions", "rtt_milliseconds")


@dataclass
class _Point:
    identity: tuple
    age: int
    operations: dict


def _read(path):
    with path.open("rb") as source:
        data = source.read(READ_BYTES + 1)
    if len(data) > READ_BYTES:
        raise ValueError("mount inventory exceeds bound")
    return data


def _unescape(field):
    # proc escapes space, tab, newline and backslash, including in a path containing a literal octal escape
    return re.sub(rb"\\(040|011|012|134)", lambda match: bytes([int(match[1], 8)]), field)


def _counter(word):
    if len(word) > 19 or not word.isdigit() or int(word) > MAX_NUMBER:
        raise ValueError("invalid kernel counter")
    return int(word)


def _mounts(proc):
    mounts = []
    for line in _read(proc / "mountinfo").split(b"\n"):
        front, _, back = line.partition(b" - ")
        fields = front.split()
        if len(fields) >= 5:
            mounts.append((_unescape(fields[4]), fields, back.split()))
    return mounts


def _identity(mounts, target):
    candidates = [mount for mount in mounts if target == mount[0] or target.startswith(mount[0].rstrip(b"/") + b"/")]
    if not candidates:
        raise ValueError("mount missing")
    candidates.sort(key=lambda mount: len(mount[0]))
    # a mount covered by another mount remains listed; every nested mount must belong to its visible parent
    for (parent_path, parent, _), (child_path, child, _) in zip(candidates, candidates[1:]):
        if parent_path == child_path or parent[0] != child[1]:
            raise ValueError("ambiguous or hidden mount")
    mountpoint, fields, filesystem = candidates[-1]
    if len(fields) < 6 or len(filesystem) != 3 or filesystem[0] != b"nfs4":
        raise ValueError("unsupported mount")
    # mount ID, device, root, mountpoint and export distinguish a replacement at the same path
    return _counter(fields[0]), fields[2], fields[3], mountpoint, filesystem[1]


def _resolved_root(root, mounts):
    target = os.fsencode(os.path.abspath(root))
    # a literal mountpoint, as on the worker, needs no traversal of the NFS client at all
    if not any(target == mountpoint for mountpoint, _, _ in mounts):
        target = os.path.realpath(target, strict=True)
    return target


def prepare_root(root, *, proc=Path("/proc/self")):
    """Resolve a configured root before dropping caches for the link command."""
    try:
        return os.fsdecode(_resolved_root(root, _mounts(proc)))
    except InterruptedError:
        raise
    except (OSError, ValueError):
        return None


def _section(data, target, device):
    found = None
    for block in data.split(b"\ndevice "):
        header, _, body = block.partition(b"\n")
        fields = header.removeprefix(b"device ").split()
        if len(fields) >= 4 and _unescape(fields[3]) == target:
            if found is not None or len(fields) != 8 or fields[:3] != [device, b"mounted", b"on"] or \
                    fields[4:7] != [b"with", b"fstype", b"nfs4"] or fields[7] not in (b"statvers=1.0", b"statvers=1.1"):
                raise ValueError("invalid mount statistics header")
            found = body
    if found is None:
        raise ValueError("mount statistics missing")
    return found


def _point(section, identity):
    age, nfs42, rpc4, per_op = None, False, False, False
    operations = {}
    for line in section.split(b"\n"):
        fields = line.split()
        if not fields:
            continue
        if per_op:
            if len(fields) not in (9, 10) or not re.fullmatch(rb"[A-Z0-9_]{1,40}:", fields[0]):
                raise ValueError("invalid RPC procedure row")
            name = fields[0][:-1].decode("ascii")
            values = [_counter(value) for value in fields[1:]]
            if name in operations or values[1] < values[0]:
                raise ValueError("invalid RPC procedure counters")
            # Linux emits completed calls, transmissions, major timeouts, bytes sent/received,
            # queue time, cumulative RTT and execute time (milliseconds), then optionally errors
            operations[name] = values[0], values[1], values[6]
        elif fields[0] == b"opts:":
            nfs42 = b"vers=4.2" in b"".join(fields[1:]).split(b",")
        elif fields[0] == b"age:" and len(fields) == 2:
            age = _counter(fields[1])
        elif fields[:3] == [b"RPC", b"iostats", b"version:"]:
            rpc4 = fields[3:4] in ([b"1.0"], [b"1.1"]) and fields[4:6] == [b"p/v:", b"100003/4"]
        elif fields == [b"per-op", b"statistics"]:
            per_op = True
    if age is None or not nfs42 or not rpc4 or not operations:
        raise ValueError("incomplete NFSv4.2 statistics")
    return _Point(identity, age, operations)


def snapshot(root, *, proc=Path("/proc/self"), resolve_after=False):
    """Read the current mount of a prepared root, or resolve the configured root after reading counters."""
    if root is None:
        return None
    try:
        mounts = _mounts(proc)
        stats = _read(proc / "mountstats")
        # after the command, resolving an alias must not add its lookups to the observed RPCs
        target = _resolved_root(root, mounts) if resolve_after else os.fsencode(os.path.abspath(root))
        identity = _identity(mounts, target)
        mountpoint, export = identity[-2:]
        section = _section(stats, mountpoint, export)
        if _identity(_mounts(proc), target) == identity:
            return _point(section, identity)
    except InterruptedError:
        raise
    except (OSError, ValueError):
        pass
    return None


def difference(before, after):
    """Report command-window deltas. Totals include every observed procedure on the client mount."""
    if before is None or after is None:
        return dict(status="unavailable")
    if before.identity != after.identity or after.age < before.age:
        return dict(status="mount_changed")
    if before.operations.keys() != after.operations.keys():
        return dict(status="unavailable")
    operations, total = {}, dict.fromkeys(COUNTERS, 0)
    for name, old in before.operations.items():
        completed, sent, rtt = (end - start for start, end in zip(old, after.operations[name]))
        if completed < 0 or sent < completed or rtt < 0:
            return dict(status="counter_reset")
        delta = dict(completed=completed, transmissions=sent, retransmissions=sent - completed, rtt_milliseconds=rtt)
        for key in COUNTERS:
            total[key] += delta[key]
        if name in OPERATIONS:
            operations[name] = delta
    if max(total.values()) > MAX_NUMBER:
        return dict(status="unavailable")
    return dict(status="available", total=total, operations=operations)
