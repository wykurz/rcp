"""Versioned, bounded public diagnostics derived from retirement-bound worker bytes.

These fields supplement an unchanged frozen qualification record. They never
qualify a run, change series IDs, publish raw logs, or execute archive contents.
"""

import math
from html import escape
import re

from benchmarks import nfs_rpc, work_progress
from benchmarks.operations import expected_counts
from .common import digest, require
from .json_bytes import encoded

SCHEMA = "rcp-public-resource-diagnostics-v1"
# the same document for a matrix run: every copy also names its scenario, and one build is measured
MATRIX_SCHEMA = "rcp-public-resource-diagnostics-v2"
# and for a matrix run with remote cells: a remote copy is three processes, so every copy also
# carries `roles`, the figures of the source and destination daemons (null for a local copy)
REMOTE_SCHEMA = "rcp-public-resource-diagnostics-v3"
# and for a matrix run that also times the other tools: every trial names what its own cell ran, and
# only a remote copy carries `roles` (a tool run is one process, as a local copy is)
TOOLS_SCHEMA = "rcp-public-resource-diagnostics-v4"
# and for a matrix run with cells on other storage: a copy between two mounts of a network
# filesystem or between two roots in memory is one process, as a local copy is; the tools follow
# those copies too; and `other_storage` says what that storage was in this run
STORAGE_SCHEMA = "rcp-public-resource-diagnostics-v5"
# the documents in which a trial carries `roles`
ROLE_SCHEMAS = (REMOTE_SCHEMA, TOOLS_SCHEMA, STORAGE_SCHEMA)
MATRIX_VARIANT = "rcp-default"
LOCAL = "local"
REMOTE = "remote"
# the copies that keep their trees beside the arrays, named as their scenarios and their storage are
OTHER_STORAGE = ("nfs", "memory")
# the cells of the other tools, named after the copies each follows: its tool, and what that
# tool ran as the public record of such a run names it. The documents before there was other
# storage have the cells after local and remote copies
_TOOLS = (("rcmp", "rcmp-size"), ("rlink", "rlink-default"), ("rrm", "rrm-default"))
TOOL_CELLS = {copies + "-" + tool: (tool, variant) for copies in (LOCAL, REMOTE) for tool, variant in _TOOLS}
TOOL_VARIANTS = {scenario: variant for scenario, (_, variant) in TOOL_CELLS.items()}
STORAGE_TOOL_CELLS = {copies + "-" + tool: (tool, variant) for copies in (LOCAL, REMOTE, *OTHER_STORAGE)
                      for tool, variant in _TOOLS}
# what a run may say of its other storage: the figures of each kind and the largest each may be
STORAGE_FIGURES = {"nfs": (("threads", 65536), ("delay_microseconds", 10_000_000),
                           ("round_trip_microseconds", 60_000_000)),
                   "memory": (("size_bytes", 1 << 50), ("inodes", 1 << 40))}
NFS_VERSIONS = ("3", "4.0", "4.1", "4.2")
# how the worker sets up that storage (other_storage.py), for the pages that describe it. The
# server does not ship the worker's module, so its own table must say the same
CONFIGURED_STORAGE = dict(nfs_version="4.2", nfs_delay_microseconds=2500, nfs_threads=256,
                          memory_bytes=64 * 1024 ** 3)
ROLES = ("source", "destination")
# with every figure of every sampled phase present and large, the largest suite (40 cells on
# three kinds of storage, 205 phases) fills 488 kB of this; the one before it filled 376 kB of
# the 512 KiB the bound was then. A document over the bound loses its sampled detail
MAX_DOCUMENT = 768 * 1024
MAX_TRIALS = 128
# the file sets of one run: every one of the arrays, of a network filesystem and of memory
MAX_CASES = 6
MAX_NUMBER = (1 << 63) - 1
PROCESS_SCOPE = ("One local copy command, its threads and waited descendants. CPU seconds may exceed "
                 "wall time; peak RSS is a per-command peak, not host memory or a sum of processes.")
# figures of a run with remote cells are rcp's own (--output-format=json), never an outside measurement
REMOTE_PROCESS_SCOPE = ("Each figure is what a process reports about itself as it finishes, from the kernel's "
                        "accounting of that process. A local copy is one rcp process with its threads. A remote "
                        "copy is three processes: the rcp command, without the ssh clients it starts, the source "
                        "rcpd and the destination rcpd. CPU seconds may exceed wall time; peak RSS is a "
                        "per-process peak, not host memory or a sum of processes.")
TOOLS_PROCESS_SCOPE = REMOTE_PROCESS_SCOPE.replace(
    "A local copy is one rcp process with its threads. ",
    "A local copy is one rcp process with its threads. A tool run is one process of that tool with its threads. ")
STORAGE_PROCESS_SCOPE = TOOLS_PROCESS_SCOPE.replace(
    "A local copy is one rcp process with its threads. ",
    "A local copy, a copy between two mounts of a network filesystem and a copy between two roots in memory are "
    "each one rcp process with its threads. ")
SYSTEM_SCOPE = ("Sampled host/cgroup diagnostics associated with phase windows, not per-process "
                "attribution. Sampled maxima can miss peaks; cgroup lifetime peaks are not phase peaks.")
METRICS = ("user_seconds", "system_seconds", "max_rss_kib", "voluntary_switches",
           "involuntary_switches", "minor_faults", "major_faults")
STATUSES = {"complete", "partial", "unavailable"}
REASONS = {"not_recorded", "raw_unavailable", "raw_budget", "invalid_diagnostics", "not_matched", "phase_summary_budget"}
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
CASE_SHAPES = {
    "bulk-1g-1tb": dict(directory_widths=[10], files_per_leaf=100, file_size_bytes=1000000000),
    "bulk-100m-1024gb": dict(directory_widths=[10], files_per_leaf=1024, file_size_bytes=100000000),
    "bulk-1m-1024gb": dict(directory_widths=[10, 10, 10], files_per_leaf=1024, file_size_bytes=1000000),
    "tiny-1k-10240k": dict(directory_widths=[10, 10, 10, 10], files_per_leaf=1024, file_size_bytes=1000),
}
# matrix runs may also use the file sets of other storage (a network filesystem: many small
# directories; memory: the tiny files under a name of their own) and the small fixtures that
# exercise a whole run in seconds
MATRIX_CASE_SHAPES = dict(CASE_SHAPES, **{
    # the sets of the weekly suite: the four above made smaller, so that a command takes minutes
    "bulk-1g-400gb": dict(directory_widths=[10], files_per_leaf=40, file_size_bytes=1000000000),
    "bulk-100m-400gb": dict(directory_widths=[10], files_per_leaf=400, file_size_bytes=100000000),
    "bulk-1m-400gb": dict(directory_widths=[10, 10, 10], files_per_leaf=400, file_size_bytes=1000000),
    "tiny-1k-4096k": dict(directory_widths=[4, 10, 10, 10], files_per_leaf=1024, file_size_bytes=1000),
    "tiny-1k-4096k-memory": dict(directory_widths=[4, 10, 10, 10], files_per_leaf=1024, file_size_bytes=1000),
    # the sets of the weekly suite that runs: the first four made five times smaller, without the 100 MB files
    "bulk-1g-200gb": dict(directory_widths=[10], files_per_leaf=20, file_size_bytes=1000000000),
    "bulk-1m-200gb": dict(directory_widths=[10, 10, 10], files_per_leaf=200, file_size_bytes=1000000),
    "tiny-1k-2048k": dict(directory_widths=[2, 10, 10, 10], files_per_leaf=1024, file_size_bytes=1000),
    "tiny-1k-2048k-memory": dict(directory_widths=[2, 10, 10, 10], files_per_leaf=1024, file_size_bytes=1000),
    "tiny-1k-700x1024": dict(directory_widths=[7, 10, 10], files_per_leaf=1024, file_size_bytes=1000),
    "tiny-1k-10240k-memory": dict(directory_widths=[10, 10, 10, 10], files_per_leaf=1024, file_size_bytes=1000),
    "commission-64m": dict(directory_widths=[4], files_per_leaf=4, file_size_bytes=4000000),
    "commission-10k": dict(directory_widths=[10, 10], files_per_leaf=100, file_size_bytes=1000),
    "commission-10k-memory": dict(directory_widths=[10, 10], files_per_leaf=100, file_size_bytes=1000),
})
SCENARIO = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
# what a setup may say about how copies were checked: the paired rules, and the matrix rule
# that only a matrix result may claim
PAIRED_VERIFICATION = frozenset({"metadata-size-tree-v1", "streaming-content-tree-v1"})
VERIFICATION_POLICIES = PAIRED_VERIFICATION | {"exit-status-summary-v1"}
SETUP_FIELDS = {"cpu_model", "logical_cpu_count", "effective_cpu_count", "host_memory_total_bytes",
                "os", "kernel", "architecture", "source", "destination", "cache_policy",
                "verification_policy", "pairing_policy", "instrumentation"}
SIDE_FIELDS = {"filesystem", "mount_options", "layout", "member_count", "chunk_bytes", "block_bytes",
               "member_models", "member_bytes"}
PHASES = frozenset({"setup", "generation", "initial_verification", "capacity", "preparation", "cache",
    "command", "timings", "destination", "summary", "metadata", "source", "cleanup", "unattributed"})
TELEMETRY_DESCRIPTOR = dict(policy="linux-bounded-system-sampling-v1", period_seconds=10,
    max_samples=4096, max_bytes=8 * 1024 * 1024, max_seconds=8 * 3600, max_phases=192,
    scope="host aggregate and current harness cgroup with descendants; logical mount-device IO; not per-process attribution")
# a matrix run may take twelve hours and is sampled under bounds for them. The runs before that,
# and the paired harness, have the bounds above: a record names the ones it was sampled under
MATRIX_TELEMETRY_DESCRIPTOR = dict(TELEMETRY_DESCRIPTOR, max_samples=5120, max_bytes=16 * 1024 * 1024,
                                   max_seconds=12 * 3600)
# a matrix run marks a phase for every step of every cell, and a suite with cells on other storage
# marks more of them than the journals before had room for
STORAGE_TELEMETRY_DESCRIPTOR = dict(MATRIX_TELEMETRY_DESCRIPTOR, max_phases=384)
TELEMETRY_DESCRIPTORS = (TELEMETRY_DESCRIPTOR, MATRIX_TELEMETRY_DESCRIPTOR, STORAGE_TELEMETRY_DESCRIPTOR)
# a summary of samples holds no more than the widest journal can: the reader needs no argument
# about how long one phase, or the phases outside any copy, may take
MAX_JOURNAL_SAMPLES = max(item["max_samples"] for item in TELEMETRY_DESCRIPTORS)
MAX_JOURNAL_SECONDS = max(item["max_seconds"] for item in TELEMETRY_DESCRIPTORS)
# the longest allocation a run can have
MAX_ALLOCATION_SECONDS = 12 * 3600
PHASE_METRICS = frozenset({"cpu_usage_usec", "read_bytes", "write_bytes", "memory_current_bytes",
    "cgroup_run_memory_peak_bytes", "host_cpu_total_ticks", "host_cpu_idle_ticks", "host_cpu_iowait_ticks",
    "host_memory_available_bytes", "source_read_sectors", "source_write_sectors", "source_io_milliseconds",
    "destination_read_sectors", "destination_write_sectors", "destination_io_milliseconds"} | {
        scope + "_" + kind + "_pressure_" + category + "_usec"
        for scope in ("host", "cgroup") for kind in ("cpu", "memory", "io") for category in ("some", "full")})


def number(value, *, integer=False, maximum=MAX_NUMBER):
    return type(value) in ((int,) if integer else (int, float)) and \
        0 <= value <= maximum and math.isfinite(value)


def _label(value, kind):
    if not isinstance(value, str):
        return None
    patterns = {
        "cpu_model": r"(?:Intel|AMD|Amazon|ARM|Apple|Generic)[A-Za-z0-9 ._()+@-]{0,150}",
        "token": r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}",
        "model": r"(?:Amazon EC2 NVMe Instance Storage|Amazon Elastic Block Store|nvme)",
    }
    return value if re.fullmatch(patterns[kind], value) else None


def _integer(value, maximum=MAX_NUMBER):
    return value if number(value, integer=True, maximum=maximum) else None


def _sampled(context):
    """The bounds a run says it was sampled under, when a runner has had them; None otherwise.

    The answer is this module's copy of those bounds: nothing of the record's own object is published.
    """
    value = context.get("system_telemetry") if isinstance(context, dict) else None
    return next((dict(known) for known in TELEMETRY_DESCRIPTORS if value == known), None)


def _new_setup_value(value, key):
    field = value.get(key, {}) if isinstance(value, dict) else {}
    return field.get("value") if isinstance(field, dict) and field.get("status") == "available" else None


def _mapping(value):
    """A part of a report that should be a mapping, or an empty one when it is anything else."""
    return value if isinstance(value, dict) else {}


def _side(environment, worker, side):
    # a run that keeps no file set on the arrays reports no capacity of them, and a report is
    # read part by part: a part that is not what it should be costs its own figures and no more
    filesystem = _mapping(_mapping(environment.get("filesystem")).get(side))
    storage = _mapping(worker.get("storage"))
    array = _mapping(_mapping(storage.get("arrays")).get(side))
    capacity = _mapping(_mapping(storage.get("capacity")).get(side))
    options = filesystem.get("mount_options", [])
    options = sorted(x for x in options if isinstance(x, str) and
        (x in {"rw", "ro", "nodev", "nosuid", "noexec", "relatime", "noatime", "discard"} or
         re.fullmatch(r"stripe=[0-9]{1,8}", x))) if isinstance(options, list) else []
    members = array.get("members")
    models = array.get("member_models", [])
    sizes = array.get("member_bytes", [])
    return dict(filesystem=_label(filesystem.get("filesystem_type"), "token"),
        mount_options=options,
        layout="two-disjoint-raid0" if storage.get("layout") == "two-disjoint-raid0-ext4-v1" else None,
        member_count=len(members) if isinstance(members, list) and 0 < len(members) <= 64 else None,
        chunk_bytes=_integer(array.get("chunk_bytes"), 2 ** 30),
        block_bytes=_integer(capacity.get("block_size"), 2 ** 20),
        member_models=[item for item in (_label(x, "model") for x in models) if item is not None][:64]
            if isinstance(models, list) else [],
        member_bytes=[x for x in sizes if number(x, integer=True)][:64] if isinstance(sizes, list) else [])


def setup(raw, worker, *, policies=PAIRED_VERIFICATION):
    context = raw.get("context", {}) if isinstance(raw, dict) else {}
    context = context if isinstance(context, dict) else {}
    environment = context.get("environment", {})
    environment = environment if isinstance(environment, dict) else {}
    observed = raw.get("system_setup", {})
    value = dict(
        cpu_model=_label(_new_setup_value(observed, "cpu_model") or environment.get("cpu_model"), "cpu_model"),
        logical_cpu_count=_integer(_new_setup_value(observed, "logical_cpu_count"), 65536),
        effective_cpu_count=_integer(environment.get("effective_parallelism"), 65536),
        host_memory_total_bytes=_integer(_new_setup_value(observed, "host_memory_total_bytes")),
        os=_label(_new_setup_value(observed, "os"), "token"),
        kernel=_label(_new_setup_value(observed, "kernel") or environment.get("kernel"), "token"),
        architecture=_label(_new_setup_value(observed, "architecture") or environment.get("architecture"), "token"),
        source=_side(environment, worker, "source"), destination=_side(environment, worker, "destination"),
        cache_policy=context.get("cache_policy") if context.get("cache_policy") in
            {"linux-drop-caches", "source-warm", "source-verified"} else None,
        verification_policy=context.get("verification", {}).get("policy")
            if isinstance(context.get("verification"), dict) and
            context["verification"].get("policy") in policies else None,
        pairing_policy="adjacent-balanced-sha256-v1" if isinstance(context.get("pairing"), dict) and
            context["pairing"].get("policy") == "adjacent-balanced-sha256-v1" else None,
        instrumentation=_sampled(context))
    return value


def setup_id(value):
    validate_setup(value)
    return digest(encoded(value))


def validate_setup(value):
    require(isinstance(value, dict) and set(value) == SETUP_FIELDS, "invalid public setup")
    for key, kind in (("cpu_model", "cpu_model"), ("os", "token"), ("kernel", "token"), ("architecture", "token")):
        require(value[key] is None or _label(value[key], kind) == value[key], "invalid public setup label")
    for key in ("logical_cpu_count", "effective_cpu_count", "host_memory_total_bytes"):
        require(value[key] is None or number(value[key], integer=True), "invalid public setup size")
    for side in ("source", "destination"):
        item = value[side]
        require(isinstance(item, dict) and set(item) == SIDE_FIELDS, "invalid public storage setup")
        require(item["filesystem"] is None or _label(item["filesystem"], "token") == item["filesystem"],
                "invalid public filesystem")
        require(item["layout"] in {None, "two-disjoint-raid0"}, "invalid public layout")
        require(isinstance(item["mount_options"], list) and len(item["mount_options"]) <= 16 and
                item["mount_options"] == sorted(set(item["mount_options"])) and
                all(isinstance(x, str) and (x in {"rw", "ro", "nodev", "nosuid", "noexec", "relatime", "noatime", "discard"}
                    or re.fullmatch(r"stripe=[0-9]{1,8}", x)) for x in item["mount_options"]),
                "invalid public mount options")
        for key in ("member_count", "chunk_bytes", "block_bytes"):
            require(item[key] is None or number(item[key], integer=True), "invalid public storage size")
        require(isinstance(item["member_models"], list) and len(item["member_models"]) <= 64 and
                all(_label(x, "model") == x for x in item["member_models"]), "invalid public model")
        require(isinstance(item["member_bytes"], list) and len(item["member_bytes"]) <= 64 and
                all(number(x, integer=True) for x in item["member_bytes"]), "invalid public member size")
    require(value["cache_policy"] in {None, "linux-drop-caches", "source-warm", "source-verified"} and
            (value["verification_policy"] is None or value["verification_policy"] in VERIFICATION_POLICIES) and
            value["pairing_policy"] in {None, "adjacent-balanced-sha256-v1"} and
            value["instrumentation"] in (None, *TELEMETRY_DESCRIPTORS), "invalid public setup policy")
    return value


GAUGES = frozenset({"memory_current_bytes", "cgroup_run_memory_peak_bytes", "host_memory_available_bytes"})


def _phase_metric(key, value, *, public):
    if not isinstance(value, dict) or value.get("status") not in {"available", "counter_reset", "unavailable"}:
        return None
    status = value["status"]
    if status == "unavailable":
        return dict(status=status)
    if key in GAUGES:
        if status != "available" or any(not number(value.get(field), integer=True)
                for field in ("minimum", "maximum")) or value["minimum"] > value["maximum"]:
            return None
        if not public and (any(not number(value.get(field), integer=True) for field in ("first", "last")) or
                not value["minimum"] <= min(value["first"], value["last"]) <=
                    max(value["first"], value["last"]) <= value["maximum"]):
            return None
        return dict(status=status, minimum=value["minimum"], maximum=value["maximum"])
    delta = value.get("delta")
    if status == "counter_reset":
        if delta is not None:
            return None
    elif not number(delta, integer=True):
        return None
    if not public:
        required = ("first", "last", "minimum", "maximum")
        if any(not number(value.get(field), integer=True) for field in required) or not (
                value["minimum"] <= min(value["first"], value["last"]) <=
                max(value["first"], value["last"]) <= value["maximum"]):
            return None
        if status == "available" and delta != value["last"] - value["first"]:
            return None
    return dict(status=status, delta=delta)


def _system(value, *, public=False):
    """Keep counter deltas and gauge bounds; raw repeated counters stay private."""
    unavailable = dict(status="unavailable", samples=0, phases=[])
    if not isinstance(value, dict) or value.get("status") not in {"complete", "partial", "unavailable", "truncated"} or \
            not number(value.get("samples"), integer=True, maximum=MAX_JOURNAL_SAMPLES):
        return unavailable
    phases = value.get("phases")
    if not isinstance(phases, list) or len(phases) > 192:
        return unavailable
    output = []
    for phase in phases:
        if not isinstance(phase, dict) or phase.get("phase") not in PHASES or \
                phase.get("status") not in {"observed", "running", "interrupted"} or \
                not number(phase.get("elapsed_seconds"), maximum=MAX_JOURNAL_SECONDS) or \
                not number(phase.get("samples"), integer=True, maximum=MAX_JOURNAL_SAMPLES):
            return unavailable
        indexes = [phase.get(key) for key in ("case_index", "trial_index")]
        if any(x is not None and not number(x, integer=True, maximum=1000000) for x in indexes):
            return unavailable
        metrics = phase.get("metrics")
        if not isinstance(metrics, dict):
            return unavailable
        selected = {}
        for key in PHASE_METRICS:
            if key not in metrics:
                continue
            metric = _phase_metric(key, metrics[key], public=public)
            if metric is None:
                return unavailable
            selected[key] = metric
        output.append(dict(phase=phase["phase"], case_index=indexes[0], trial_index=indexes[1],
            status=phase["status"], elapsed_seconds=phase["elapsed_seconds"], samples=phase["samples"], metrics=selected))
    return dict(status=value["status"], samples=value["samples"], phases=output)


def _resources(trial):
    value = trial.get("resources", {})
    metrics = value.get("metrics", {}) if isinstance(value, dict) else {}
    if trial.get("status") != "ok" or trial.get("ok") is not True or value.get("status") != "complete" or \
            not isinstance(metrics, dict) or type(metrics.get("exit_code")) is not int or metrics["exit_code"] != 0 or \
            any(not number(metrics.get(key), integer=key not in {"user_seconds", "system_seconds"}) for key in METRICS):
        return None
    return {key: metrics[key] for key in METRICS}


def _rpc_counters(value):
    if not isinstance(value, dict) or any(not number(value.get(key), integer=True) for key in nfs_rpc.COUNTERS) or \
            value["transmissions"] != value["completed"] + value["retransmissions"]:
        return None
    return {key: value[key] for key in nfs_rpc.COUNTERS}


def _nfs_window(value, links):
    """Project allowlisted mount counters; a malformed observation costs only this window."""
    value = _mapping(value)
    status = value.get("status")
    if status in ("counter_reset", "mount_changed"):
        return dict(status=status)
    unavailable = dict(status="unavailable")
    total, reported = _rpc_counters(value.get("total")), value.get("operations")
    if status != "available" or total is None or not isinstance(reported, dict):
        return unavailable
    operations = {}
    for name in nfs_rpc.OPERATIONS:
        if name in reported:
            counters = _rpc_counters(reported[name])
            if counters is None:
                return unavailable
            operations[name] = counters
    if any(total[key] < sum(op[key] for op in operations.values()) for key in nfs_rpc.COUNTERS):
        return unavailable
    return dict(status="available", hard_links_created=links, total=total, operations=operations)


def _link_count(value, case):
    """Accept the reported success count only when it matches the public file set's expected count."""
    return value if type(value) is int and value == expected_counts(MATRIX_CASE_SHAPES[case])["files"] else None


def _matrix(record):
    """A public record that carries cells is a matrix run; its diagnostics name each copy's scenario."""
    return "cells" in record


def _cells(record):
    return {(cell["case_id"], cell["scenario"]) for cell in record["cells"]}


def _schema(record):
    """The document a record gets: fixed by the record, so a stored one never changes version."""
    if not _matrix(record):
        return SCHEMA
    scenarios = {cell["scenario"] for cell in record["cells"]}
    if _other_kinds(record):
        return STORAGE_SCHEMA
    if scenarios & set(TOOL_VARIANTS):
        return TOOLS_SCHEMA
    return REMOTE_SCHEMA if scenarios - {LOCAL} else MATRIX_SCHEMA


def _other_kinds(record):
    """The storage beside the arrays that a record has cells on, in the order of its cells."""
    kinds = (cell["scenario"].split("-", 1)[0] for cell in record["cells"])
    return tuple(dict.fromkeys(kind for kind in kinds if kind in OTHER_STORAGE))


def _process_scope(schema):
    return {REMOTE_SCHEMA: REMOTE_PROCESS_SCOPE, TOOLS_SCHEMA: TOOLS_PROCESS_SCOPE,
            STORAGE_SCHEMA: STORAGE_PROCESS_SCOPE}.get(schema, PROCESS_SCOPE)


def _tool_cells(schema):
    """The tool cells a document of this version may hold: each with its tool and what that tool ran."""
    return {TOOLS_SCHEMA: TOOL_CELLS, STORAGE_SCHEMA: STORAGE_TOOL_CELLS}.get(schema, {})


def _variant(schema, scenario):
    """What a trial of this scenario ran: the one copy variant, or in a run with tool cells its tool's."""
    return _tool_cells(schema).get(scenario, (None, MATRIX_VARIANT))[1]


def _daemons(schema, scenario):
    """Whether a trial of this scenario names the two daemons of a remote copy.

    Before there were tool cells every scenario but the local one was a remote copy.
    """
    return scenario == REMOTE if schema in (TOOLS_SCHEMA, STORAGE_SCHEMA) else scenario != LOCAL


def _storage_entry(kind, item):
    """One published entry of a run's other storage, when `item` is exactly one; None otherwise."""
    if not isinstance(item, dict) or item.get("status") not in ("prepared", "unavailable", "not_recorded"):
        return None
    if item["status"] != "prepared":
        return dict(item) if set(item) == {"status"} else None
    figures = STORAGE_FIGURES[kind]
    if set(item) != {"status", *(name for name, _ in figures), *(("version",) if kind == "nfs" else ())} or \
            any(not number(item[name], integer=True, maximum=limit) for name, limit in figures) or \
            kind == "nfs" and item["version"] not in NFS_VERSIONS:
        return None
    return dict(item)


def _other_storage(record, worker):
    """What the worker reported of the storage beside the arrays, for the kinds this run has cells on.

    Only the known figures are taken, each as a bounded number: nothing else of the report is published.
    """
    reported = worker.get("other_storage")
    reported = reported if isinstance(reported, dict) else {}
    result = {}
    for kind in _other_kinds(record):
        item, entry = reported.get(kind), None
        if isinstance(item, dict) and "error" in item:
            entry = dict(status="unavailable")
        elif isinstance(item, dict):
            taken = dict(status="prepared", **{name: item.get(name) for name, _ in STORAGE_FIGURES[kind]})
            if kind == "nfs":
                seconds = item.get("round_trip_seconds")
                taken.update(version=item.get("version"),
                             round_trip_microseconds=round(seconds * 1_000_000) if number(seconds, maximum=60) else None)
            entry = _storage_entry(kind, taken)
        result[kind] = entry or dict(status="not_recorded")
    return result


def _complete(trials):
    """Whether every copy has every figure it could have: its command's and, if remote, each daemon's."""
    return all(trial["metrics"] is not None and
               all(value is not None for value in (trial.get("roles") or {}).values()) for trial in trials)


def unavailable(record, reason="not_recorded"):
    require(reason in REASONS, "invalid diagnostic availability reason")
    value = dict(schema=_schema(record),
        run_key=record["run_key"], retirement_sha256=record["retirement_sha256"],
        archive_sha256=None, status="unavailable", reason=reason, process_scope=_process_scope(_schema(record)),
        system_scope=SYSTEM_SCOPE, setup=None, setup_id=None, trials=[], cases=[], system=None,
        limits=dict(allocation_seconds=None, command_timeout_seconds=None))
    if value["schema"] == STORAGE_SCHEMA:
        # the document of such a run always says of each storage what is known: here, nothing
        value["other_storage"] = {kind: dict(status="not_recorded") for kind in _other_kinds(record)}
    return validate(value, record)




def from_archive(record, archive):
    """Read bounded JSON members only; the caller authenticates archive bytes first."""
    from . import results
    selected, _, _ = results.members(archive)
    raw = results.json_value(selected[results.RESULT], results.MAX_JSON)
    worker = results.json_value(selected["worker.json"])
    return derive(record, archive, raw, worker)

def _definition_status(item, expected):
    strict_types = (type(item.get("files_per_leaf")) is int and type(item.get("file_size_bytes")) is int
        and isinstance(item.get("directory_widths"), list)
        and all(type(width) is int for width in item["directory_widths"]))
    return ("shape-unavailable" if not set(expected) <= set(item) else
        "recorded" if strict_types and all(item[field] == value for field, value in expected.items()) else "shape-mismatch")


def case_progress(raw, selected):
    """Definitions and recorded activity are diagnostics, never per-case qualification."""
    definitions = raw.get("cases", [])
    definitions = definitions if isinstance(definitions, list) and len(definitions) <= 16 else []
    order, definition_status, definition_indexes = [], {}, {}
    for original_index, item in enumerate(definitions):
        if not isinstance(item, dict) or item.get("id") not in CASE_SHAPES or item["id"] in order:
            continue
        key = item["id"]
        order.append(key)
        definition_indexes[key] = original_index
        definition_status[key] = _definition_status(item, CASE_SHAPES[key])
    for trial in selected:
        if trial["case_id"] not in order:
            order.append(trial["case_id"])
            definition_status[trial["case_id"]] = "observed-only"
    phases = []
    context = raw.get("context")
    sampled = _sampled(context) is not None
    if sampled:
        for summary in [raw.get("system_telemetry"), *[x.get("system_telemetry") for x in raw.get("trials", [])]]:
            phases += _system(summary)["phases"]
    result = []
    for index, key in enumerate(order):
        trials = [trial for trial in raw.get("trials", []) if isinstance(trial, dict) and trial.get("case_id") == key]
        boundary = key in definition_indexes and any(isinstance(phase, dict) and
            type(phase.get("case_index")) is int and phase["case_index"] == definition_indexes[key] and
            phase.get("phase") in PHASES for phase in phases)
        result.append(dict(case_id=key, definition_status=definition_status[key], copy_trials=len(trials),
            successful_copies=sum(trial.get("status") == "ok" and trial.get("ok") is True for trial in trials),
            phase_boundary_recorded=boundary))
    return result


def derive(record, archive, raw, worker):
    """Derive supplementary diagnostics after the caller verifies the archive digest."""
    require(isinstance(archive, bytes) and isinstance(raw, dict) and isinstance(worker, dict),
            "invalid diagnostic source")
    if _matrix(record):
        return _derive_matrix(record, archive, raw, worker)
    trials = raw.get("trials")
    if not isinstance(trials, list) or len(trials) > MAX_TRIALS:
        return unavailable(record, "invalid_diagnostics")
    # matching frozen samples prevents a companion from changing historical timing identity.
    grouped = {}
    selected = []
    context = raw.get("context", {})
    sampled = _sampled(context) is not None
    for trial in trials:
        if not isinstance(trial, dict):
            return unavailable(record, "invalid_diagnostics")
        case, variant, iteration = (trial.get(k) for k in ("case_id", "variant_id", "iteration"))
        if not isinstance(case, str) or case not in CASE_SHAPES or \
                variant not in {"rcp-default", "rcp-baseline"} or \
                not number(iteration, integer=True, maximum=100) or iteration == 0:
            return unavailable(record, "invalid_diagnostics")
        resource = _resources(trial)
        if trial.get("status") == "ok":
            if not number(trial.get("elapsed_seconds")):
                return unavailable(record, "invalid_diagnostics")
            grouped.setdefault((case, variant), []).append(trial["elapsed_seconds"])
        selected.append(dict(case_id=case, variant_id=variant, iteration=iteration,
            status="complete" if resource is not None else "unavailable", metrics=resource,
            system=_system(trial.get("system_telemetry") if sampled else None)))
    if record["benchmark_valid"]:
        expected = {(item["case_id"], item["variant_id"]): item["samples"] for item in record["measurements"]}
        if grouped != expected:
            return unavailable(record, "not_matched")
    return _assemble(record, archive, raw, worker, selected, case_progress(raw, selected), sampled)


def _derive_matrix(record, archive, raw, worker):
    """Report each copy of a matrix run by file set and scenario; unfinished cells keep their copies."""
    from benchmarks import matrix
    try:
        matrix.validate(raw)
    except matrix.Invalid:
        return unavailable(record, "invalid_diagnostics")
    cells, file_sets = raw["cells"], raw["file_sets"]
    if sum(len(cell["copies"]) for cell in cells) > MAX_TRIALS or len(file_sets) > MAX_CASES or \
            any(item["case"]["id"] not in MATRIX_CASE_SHAPES for item in file_sets):
        return unavailable(record, "invalid_diagnostics")
    # copies are published only for the cells the collector reported. A result that failed its
    # contract has none, so nothing it says about itself, names included, reaches a page.
    if any(key not in _cells(record) for key in ((cell["case_id"], cell["scenario"]) for cell in cells)):
        return unavailable(record, "not_matched")
    sampled = _sampled(raw["context"]) is not None
    grouped, selected, phases = {}, [], _system(raw.get("system_telemetry") if sampled else None)["phases"]
    schema = _schema(record)
    with_roles = schema in ROLE_SCHEMAS
    for cell in cells:
        for item in cell["copies"]:
            done = item["status"] == "ok"
            def figures(recorded):
                return _resources(dict(status=item["status"], ok=done,
                                       resources=recorded if isinstance(recorded, dict) else {}))
            resource = figures(item.get("resources"))
            if done:
                grouped.setdefault((cell["case_id"], cell["scenario"]), []).append(item["elapsed_seconds"])
            system = _system(item.get("system_telemetry") if sampled else None)
            phases += system["phases"]
            trial = dict(case_id=cell["case_id"], variant_id=_variant(schema, cell["scenario"]),
                scenario=cell["scenario"], iteration=item["iteration"],
                status="complete" if resource is not None else "unavailable", metrics=resource, system=system)
            tool = _tool_cells(schema).get(cell["scenario"], ("rcp",))[0]
            if sampled and tool in work_progress.METRICS and "work_progress" in item:
                files = expected_counts(MATRIX_CASE_SHAPES[cell["case_id"]])["files"]
                seconds = item.get("elapsed_seconds")
                duration = round(seconds * 1000) if number(seconds, maximum=MAX_ALLOCATION_SECONDS) else 0
                trial["work_progress"] = work_progress.public(item["work_progress"], work_progress.METRICS[tool],
                                                               files, duration)
            if sampled and cell["scenario"] == "nfs-rlink" and "nfs_rpc" in item:
                links = _mapping(item.get("tool_summary")).get("hard_links_created") if done else None
                trial["nfs_rpc"] = _nfs_window(item["nfs_rpc"], _link_count(links, cell["case_id"]))
            if with_roles:
                trial["roles"] = None
                if _daemons(schema, cell["scenario"]):
                    daemons = item.get("role_resources")
                    daemons = daemons if isinstance(daemons, dict) else {}
                    # a copy without its own figures publishes none for its daemons either
                    trial["roles"] = {role: figures(daemons.get(role)) if resource is not None else None
                                      for role in ROLES}
            selected.append(trial)
    # only completed cells are published; their samples must be exactly the copies recorded here
    if record["benchmark_valid"] and any(
            grouped.get((item["case_id"], item["scenario"])) != item["samples"] for item in record["measurements"]):
        return unavailable(record, "not_matched")
    progress = []
    for index, file_set in enumerate(file_sets):
        key = file_set["case"]["id"]
        copies = [item for cell in cells if cell["case_id"] == key for item in cell["copies"]]
        progress.append(dict(case_id=key, definition_status=_definition_status(file_set["case"], MATRIX_CASE_SHAPES[key]),
            copy_trials=len(copies), successful_copies=sum(item["status"] == "ok" for item in copies),
            phase_boundary_recorded=any(phase["case_index"] == index for phase in phases)))
    return _assemble(record, archive, raw, worker, selected, progress, sampled)


def _assemble(record, archive, raw, worker, selected, cases, sampled):
    observed = setup(raw, worker, policies=VERIFICATION_POLICIES if _matrix(record) else PAIRED_VERIFICATION)
    window = worker.get("window", {})
    window = window if isinstance(window, dict) else {}
    limits = dict(allocation_seconds=_integer(window.get("runtime_seconds"), MAX_ALLOCATION_SECONDS),
                  command_timeout_seconds=_integer(worker.get("command_timeout_seconds"), 2 * 3600))
    value = dict(schema=_schema(record),
        run_key=record["run_key"], retirement_sha256=record["retirement_sha256"],
        archive_sha256=digest(archive), status="complete" if selected and _complete(selected)
            else "partial" if selected else "unavailable",
        reason=None if selected else "not_recorded", process_scope=_process_scope(_schema(record)),
        system_scope=SYSTEM_SCOPE,
        setup=observed, setup_id=setup_id(observed), trials=selected, cases=cases,
        system=_system(raw.get("system_telemetry") if sampled else None), limits=limits)
    if value["schema"] == STORAGE_SCHEMA:
        value["other_storage"] = _other_storage(record, worker)
    if len(encoded(value)) > MAX_DOCUMENT:
        # Sampled detail is optional; it must not erase already recorded process metrics/setup.
        value["reason"] = "phase_summary_budget"
        value["system"] = None
        for trial in value["trials"]:
            trial["system"] = _system(None)
    return validate(value, record)


def validate(value, record):
    matrix = _matrix(record)
    schema = _schema(record)
    with_roles = schema in ROLE_SCHEMAS
    shapes = MATRIX_CASE_SHAPES if matrix else CASE_SHAPES
    require(isinstance(value, dict) and set(value) == {"schema", "run_key", "retirement_sha256",
        "archive_sha256", "status", "reason", "process_scope", "system_scope", "setup", "setup_id", "trials", "cases",
        "system", "limits", *(("other_storage",) if schema == STORAGE_SCHEMA else ())} and
        value["schema"] == _schema(record) and value["run_key"] == record["run_key"] and
        value["retirement_sha256"] == record["retirement_sha256"] and value["status"] in STATUSES and
        value["process_scope"] == _process_scope(value["schema"]) and value["system_scope"] == SYSTEM_SCOPE,
        "invalid public diagnostic identity")
    require(value["reason"] is None or value["reason"] in REASONS, "invalid diagnostic reason")
    if schema == STORAGE_SCHEMA:
        # one entry for each storage beside the arrays that the run has cells on, and no other
        other = value["other_storage"]
        require(isinstance(other, dict) and set(other) == set(_other_kinds(record)) and
                all(_storage_entry(kind, item) == item for kind, item in other.items()),
                "invalid public storage of the run")
    require(isinstance(value["limits"], dict) and set(value["limits"]) ==
            {"allocation_seconds", "command_timeout_seconds"} and
            (value["limits"]["allocation_seconds"] is None or
                number(value["limits"]["allocation_seconds"], integer=True, maximum=MAX_ALLOCATION_SECONDS)) and
            (value["limits"]["command_timeout_seconds"] is None or
                number(value["limits"]["command_timeout_seconds"], integer=True, maximum=2 * 3600)),
            "invalid diagnostic execution limits")
    require(value["archive_sha256"] is None or isinstance(value["archive_sha256"], str) and
        re.fullmatch(r"[0-9a-f]{64}", value["archive_sha256"]), "invalid diagnostic archive digest")
    require((value["setup"] is None) == (value["setup_id"] is None), "invalid diagnostic setup")
    if value["setup"] is not None:
        require(setup_id(value["setup"]) == value["setup_id"], "diagnostic setup fingerprint differs")
    require(isinstance(value["cases"], list) and len(value["cases"]) <= MAX_CASES, "too many diagnostic cases")
    case_ids = set()
    for case in value["cases"]:
        require(isinstance(case, dict) and set(case) == {"case_id", "definition_status", "copy_trials",
            "successful_copies", "phase_boundary_recorded"} and case["case_id"] in shapes and
            case["case_id"] not in case_ids and case["definition_status"] in {
                "recorded", "shape-unavailable", "shape-mismatch", "observed-only"} and
            number(case["copy_trials"], integer=True, maximum=MAX_TRIALS) and
            number(case["successful_copies"], integer=True, maximum=case["copy_trials"]) and
            type(case["phase_boundary_recorded"]) is bool, "invalid diagnostic case progress")
        case_ids.add(case["case_id"])
    require(isinstance(value["trials"], list) and len(value["trials"]) <= MAX_TRIALS, "too many public resource trials")
    seen = set()
    measurements = {(m["case_id"], m["scenario"]): m["samples"] for m in record["measurements"]} if matrix else {}
    for trial in value["trials"]:
        require(isinstance(trial, dict) and set(trial) - {"nfs_rpc", "work_progress"} == {
                    "case_id", "variant_id", "iteration", "status", "metrics", "system"} | (
                    {"scenario"} if matrix else set()) | ({"roles"} if with_roles else set()) and
                isinstance(trial["case_id"], str) and trial["case_id"] in case_ids and
                (not matrix or isinstance(trial["scenario"], str) and SCENARIO.fullmatch(trial["scenario"]) and
                    (trial["case_id"], trial["scenario"]) in _cells(record)) and
                trial["variant_id"] in ({_variant(schema, trial["scenario"])} if matrix else
                                        {"rcp-default", "rcp-baseline"}) and
                number(trial["iteration"], integer=True, maximum=100) and trial["iteration"] > 0 and
                trial["status"] in {"complete", "unavailable"}, "invalid public resource trial")
        key = (trial["case_id"], trial.get("scenario"), trial["variant_id"], trial["iteration"])
        require(key not in seen, "duplicate public resource trial")
        seen.add(key)
        if "work_progress" in trial:
            tool = _tool_cells(schema).get(trial.get("scenario"), ("rcp",))[0]
            durations = measurements.get((trial["case_id"], trial.get("scenario")))
            duration_ms = work_progress.MAX_RUN_MS
            if durations is not None:
                require(trial["iteration"] <= len(durations), "work progress has no matching measured trial")
                duration_ms = round(durations[trial["iteration"] - 1] * 1000)
            require(matrix and tool in work_progress.METRICS and value["setup"] is not None and
                    value["setup"]["instrumentation"] is not None and
                    work_progress.valid_public(trial["work_progress"], work_progress.METRICS[tool],
                        expected_counts(shapes[trial["case_id"]])["files"], duration_ms),
                    "invalid public work progress")
        if "nfs_rpc" in trial:
            window = trial["nfs_rpc"]
            links = _mapping(window).get("hard_links_created")
            require(schema == STORAGE_SCHEMA and trial["scenario"] == "nfs-rlink" and value["setup"] is not None and
                    value["setup"]["instrumentation"] is not None and (links is None or
                        _link_count(links, trial["case_id"]) is not None) and _nfs_window(window, links) == window,
                    "invalid public NFS RPC diagnostics")
        metrics = trial["metrics"]
        require((metrics is None) == (trial["status"] == "unavailable"), "invalid resource availability")
        if metrics is not None:
            require(isinstance(metrics, dict) and set(metrics) == set(METRICS) and
                all(number(metrics[key], integer=key not in {"user_seconds", "system_seconds"}) for key in METRICS),
                "invalid public process metrics")
        if with_roles:
            roles = trial["roles"]
            # a local copy and a tool run have no daemons; a remote copy names both, each with figures or without
            require((roles is None) if not _daemons(schema, trial["scenario"]) else
                    isinstance(roles, dict) and set(roles) == set(ROLES) and all(
                        role is None or metrics is not None and isinstance(role, dict) and set(role) == set(METRICS) and
                        all(number(role[key], integer=key not in {"user_seconds", "system_seconds"}) for key in METRICS)
                        for role in roles.values()), "invalid public daemon metrics")
        require(_system(trial["system"], public=True) == trial["system"], "invalid public trial system summary")
    for case in value["cases"]:
        matching = [trial for trial in value["trials"] if trial["case_id"] == case["case_id"]]
        require(len(matching) == case["copy_trials"] and
                sum(trial["status"] == "complete" for trial in matching) <= case["successful_copies"],
                "case activity differs from resource trials")
    expected_status = "complete" if value["trials"] and _complete(value["trials"]) else (
        "partial" if value["trials"] else "unavailable")
    require(value["status"] == expected_status and
            (not value["trials"] or value["archive_sha256"] is not None and value["setup"] is not None),
            "diagnostic availability differs")
    require(value["system"] is None or _system(value["system"], public=True) == value["system"], "invalid public run system summary")
    if value["setup"] is None or value["setup"]["instrumentation"] is None:
        require((value["system"] is None or value["system"]["status"] == "unavailable") and
                all(trial["system"]["status"] == "unavailable" for trial in value["trials"]),
                "system diagnostics have no recorded instrumentation contract")
    require(len(encoded(value)) <= MAX_DOCUMENT, "public diagnostics exceed bound")
    return value


def _display(value, suffix=""):
    return "not recorded" if value is None else escape(str(value)) + suffix


def _count(value):
    return None if value is None else str(value)


def _size(value):
    return None if value is None else f"{value:,} bytes"


def setup_rows(value):
    """List every observed field once as (group, label, text, kind); None text means not recorded.

    Kind says what a recorded difference between runs means for history: a
    hardware difference starts a new trend line, a software difference is
    marked on the line, and context is displayed only.
    """
    validate_setup(value)
    ram, sampler = value["host_memory_total_bytes"], value["instrumentation"]
    rows = [
        ("Machine", "CPU model", value["cpu_model"], "hardware"),
        ("Machine", "Logical CPUs", _count(value["logical_cpu_count"]), "hardware"),
        ("Machine", "Usable CPUs", _count(value["effective_cpu_count"]), "hardware"),
        ("Machine", "Architecture", value["architecture"], "hardware"),
        ("Machine", "Observed host RAM",
            None if ram is None else f"{ram / 2 ** 30:.2f} GiB ({ram:,} bytes)", "context"),
        ("System software", "OS", value["os"], "software"),
        ("System software", "Kernel", value["kernel"], "software"),
        ("Method", "Cache preparation", value["cache_policy"], "software"),
        # named for what it is: one of the recorded rules checks an exit status and a count only
        ("Method", "Copy check", value["verification_policy"], "software"),
        ("Method", "Pairing", value["pairing_policy"], "software"),
        ("Method", "System sampler",
            None if sampler is None else sampler["policy"] + " (10-second cadence)", "context"),
    ]
    for side in ("source", "destination"):
        item, group = value[side], side.title() + " storage"
        rows += [
            (group, "Filesystem", item["filesystem"], "hardware"),
            (group, "Layout", item["layout"], "hardware"),
            (group, "Devices", _count(item["member_count"]), "hardware"),
            (group, "Device models", ", ".join(item["member_models"]) or None, "hardware"),
            (group, "Device sizes", ", ".join(_size(x) for x in item["member_bytes"]) or None, "hardware"),
            (group, "RAID chunk", _size(item["chunk_bytes"]), "hardware"),
            (group, "Filesystem block", _size(item["block_bytes"]), "hardware"),
            (group, "Mount options", ", ".join(item["mount_options"]) or None, "software"),
        ]
    return rows


def _setup_table(rows):
    """Render (group, label, escaped cell) rows as one table with a body per group.

    The source and destination arrays are built alike, so identical rows are stated once.
    """
    groups = {}
    for group, label, cell in rows:
        groups.setdefault(group, []).append((label, cell))
    if groups["Source storage"] == groups["Destination storage"]:
        groups = {("Source and destination storage" if title == "Source storage" else title): cells
                  for title, cells in groups.items() if title != "Destination storage"}
    return "<table>" + "".join(
        "<tbody><tr><th colspan='2' scope='rowgroup'>" + escape(title) + "</th></tr>" + "".join(
            "<tr><th scope='row'>" + escape(label) + "</th><td>" + cell + "</td></tr>" for label, cell in cells) +
        "</tbody>" for title, cells in groups.items()) + "</table>"


def setup_html(value):
    """Render one run's observed setup; unknown historical RAM is never filled from a SKU."""
    return _setup_table((group, label, _display(text)) for group, label, text, _ in setup_rows(value))


def _observed_html(observations):
    """Merge what the runs recorded: one value where every recording run agrees."""
    seen = {}
    for item in observations:
        for group, label, text, _ in setup_rows(item["setup"]):
            values = seen.setdefault((group, label), {})
            if text is not None:
                values.setdefault(text, []).append(item)
    rows, differences = [], []
    for (group, label), values in seen.items():
        if not values:
            cell = "not recorded"
        elif len(values) == 1:
            (text, runs), = values.items()
            cell = escape(text)
            if len(runs) < len(observations):
                cell += f" <small>(recorded by {len(runs)} of {len(observations)} runs)</small>"
        else:
            cell = (" / ".join(escape(text) for text in values) +
                    " <small>(<a href='#setup-differences'>differs between runs</a>)</small>")
            parts = []
            for text, runs in values.items():
                # runs are in time order; the first and last bound each value without a long list
                links = [("<a href='" + escape(run["href"], quote=True) + "'>" + escape(run["run"]) + "</a>")
                         for run in (runs if len(runs) < 3 else (runs[0], runs[-1]))]
                where = (", ".join(links) if len(runs) < 3 else
                         f"{len(runs)} runs from " + links[0] + " to " + links[1])
                parts.append("<code>" + escape(text) + "</code> in " + where)
            differences.append("<li><strong>" + escape(group + ": " + label) + "</strong>: " +
                               "; ".join(parts) + "</li>")
        rows.append((group, label, cell))
    text = _setup_table(rows)
    if differences:
        text += ("<h3 id='setup-differences'>Differences between runs</h3><p>Only fields whose recorded "
            "values differ are listed. A field that an older run did not record is not a difference.</p>"
            "<ul>" + "".join(differences) + "</ul>")
    return text


def configuration_html(observations, *, remote=False, tools=False, elsewhere=False, sized=False, storage=""):
    """Describe the shared configured design once, then the observed values merged across runs.

    Each observation is dict(run=label, href=run page, setup=observed setup), oldest first.
    Remote copies are described once a run has a remote cell, the other tools once a run has a
    cell of one, and the storage beside the arrays (`elsewhere`) once a run has a cell there.
    The file sets are those of the weekly suite (`sized`) once it has measured something, and
    the ones before them until then. `storage` is the table of what the arrays did during the
    newest run's copies, placed under what the drives allow.
    """
    configured = CONFIGURED_STORAGE
    delay = configured["nfs_delay_microseconds"] / 1000
    beside = ("" if not elsewhere else
        "<li><strong>Network filesystem.</strong> A matrix run can also copy between two mounts of a network "
        "filesystem. The worker serves its two arrays with the kernel's NFS server on loopback and mounts them "
        f"back with NFS {configured['nfs_version']}, with {delay:g} ms added to every packet on that loopback: a "
        f"request to the server and its reply costs {2 * delay:g} ms, as an operation on a file that is not cached "
        "does on a filer. The server answers without waiting for its disks, runs "
        f"{configured['nfs_threads']} threads and hands out no delegations. Only what is measured goes through "
        "the mounts. Each run page shows the round trip its worker measured.</li>"
        "<li><strong>Memory.</strong> A matrix run can also copy between two filesystems in memory (tmpfs), "
        f"each limited to {configured['memory_bytes'] >> 30} GiB, where no operation waits for a disk.</li>")
    more_sets = ("" if not elsewhere else
        "<tr><td>1 KB on NFS</td><td>716,800</td><td>1,000</td><td>716.8 MB</td></tr>" + (
            "<tr><td>1 KB in memory</td><td>2,048,000</td><td>1,000</td><td>2.048 GB</td></tr>" if sized else
            "<tr><td>1 KB in memory</td><td>10,240,000</td><td>1,000</td><td>10.24 GB</td></tr>"))
    # the file sets of the arrays: those of the weekly suite, or the ones before them
    sets = ("<tr><td>1 GB</td><td>200</td><td>1,000,000,000</td><td>200 GB</td></tr>"
            "<tr><td>1 MB</td><td>200,000</td><td>1,000,000</td><td>200 GB</td></tr>"
            "<tr><td>1 KB</td><td>2,048,000</td><td>1,000</td><td>2.048 GB</td></tr>" if sized else
            "<tr><td>1 GB</td><td>1,000</td><td>1,000,000,000</td><td>1 TB</td></tr>"
            "<tr><td>100 MB</td><td>10,240</td><td>100,000,000</td><td>1.024 TB</td></tr>"
            "<tr><td>1 MB</td><td>1,024,000</td><td>1,000,000</td><td>1.024 TB</td></tr>"
            "<tr><td>1 KB</td><td>10,240,000</td><td>1,000</td><td>10.24 GB</td></tr>")
    sets_note = (
        "The sets are sized so that a copy, and a run of a tool where one is timed, takes a minute or two on "
        "this machine. Until the weekly suite replaced them, the sets were five times larger (a terabyte of large "
        "files, and 10,240,000 files of 1 KB), and one more held files of 100 MB: the trends of those sets are "
        "under “Earlier file sets” on the page of each kind of copy and of each tool." if sized else
        "The 1 KB file set has exactly 100 times less total logical data than the 1.024 TB sets; it still "
        "exercises over ten million files.")
    bound = (
        "The 1 GB and 1 MB file sets hold 200 GB each, about as much as the machine has memory. The kernel lets "
        "a copy run ahead of the disks by a part of that memory only, so most of such a copy goes at the pace at "
        "which the destination array takes writes. " if sized else
        "The 1 GB, 100 MB and 1 MB file sets hold about a terabyte each, several times the machine's memory, "
        "so a copy of them cannot go faster than the destination array takes writes. ")
    unbound = ("The 1 KB file set holds about 2 GB, which fits in memory many times over, and is not bound this "
               "way." if sized else
               "The 1 KB file set holds about 10 GB, which fits in memory many times over, and is not bound this "
               "way.")
    timed = ("A matrix run times the other tools on the copies of the sets of 1 KB files, in this order: "
             if sized else
             "A matrix run can also time the other tools on each local copy, and on each remote copy when its "
             "suite says so, in this order: ")
    untimed = (" A tool takes as long as its files are many, and the sets of larger files have too few of them "
               "for a run of a minute, so no tool is timed there. On the arrays <code>rcmp</code> and "
               "<code>rlink</code> follow the local copies only: a remote copy leaves the same tree, and comparing "
               "it or hard-linking it once more would measure the same again. <code>rrm</code> follows every copy, "
               "since its run is what removes it." if sized else "")
    more_sets_note = ("" if not elsewhere else
        " The file set on NFS is 700 directories of 1,024 files: a network filesystem takes one new file at a "
        "time in a directory, so many small directories are what a parallel tool can work on there.")
    caches = ("Linux cache dropping occurs before every copy." if not elsewhere else
        "Linux cache dropping occurs before every copy and, on the network filesystem, before every run of a "
        "tool: what a client still remembers of a tree decides how many requests a tool sends for it. No cache "
        "is dropped for the filesystems in memory.")
    other = ("" if not tools else
        "<li><strong>Other tools.</strong> " + timed + "<code>rcmp --metadata-compare=f:size</code> "
        "compares the copy with its source by file size, <code>rlink</code> hard-links it into a new tree beside "
        "it, and <code>rrm</code> removes it. No cache is dropped for a tool: it finds what the copy left in "
        "memory. A run of a tool counts when it exits cleanly and reports the expected counts, and reports its "
        "own CPU time and peak memory. Such a run clears every tree with that <code>rrm</code>, unmeasured: what "
        "<code>rlink</code> made and each file set that is done with. Should <code>rrm</code> fail at one, "
        "<code>rm -rf</code> clears the trees from then on and the run page says so." + untimed + "</li>")
    # what would otherwise surprise a reader: copies of a terabyte take as long as the destination array needs
    same = ("A local copy and a remote copy of the same file set that both keep up with the array therefore take "
            "almost the same time. " if remote else "")
    commissioning = ("The <code>matrix-commission-…</code> suites copy" if remote else
                     "The <code>matrix-commission-local</code> suite copies")
    copy = (
        "<li><strong>Copy.</strong> The copy datapath is local on one host, not a network copy; package and "
        "evidence transfers happen outside copy measurement. RCP uses its version-specific default settings "
        "apart from summary output, with no supplied bandwidth or operations throttle.</li>" if not remote else
        "<li><strong>Copy.</strong> Every copy runs on one host, from one array to the other; package and "
        "evidence transfers happen outside copy measurement. RCP uses its version-specific default settings "
        "apart from summary output, with no supplied bandwidth or operations throttle.</li>"
        "<li><strong>Remote copies.</strong> A matrix run can also copy a file set through rcp's remote path on "
        "the same host: <code>rcp --force-remote</code> starts a source and a destination <code>rcpd</code> over "
        "SSH, on a private sshd that the benchmark runs on loopback, and the daemons exchange the data over "
        "loopback TCP with rcp's default encryption. Every address is 127.0.0.1. No network hardware and no added "
        "latency are involved, so this measures the remote protocol, not a network. The <code>rcp</code> command "
        "and each daemon report their own CPU time and peak memory.</li>")
    repeats = "of each file set under each scenario" if remote else "of each file set"
    text = (
        "<section aria-labelledby='machine'><h2 id='machine'>Machine and storage</h2><ul>"
        "<li><strong>Instance.</strong> The configured AWS worker is a Linux x86-64 <code>c5d.24xlarge</code>. "
        "The AWS instance-type specification is 96 vCPUs (48 cores), 192 GiB RAM and four 900 GB local "
        "SSD/NVMe devices; this is provider configuration, not recovered guest RAM evidence.</li>"
        "<li><strong>Benchmark data.</strong> Benchmark data is placed on local NVMe instance-store storage, "
        "split into two disjoint two-member RAID0 arrays, one for source and one for destination.</li>"
        "<li><strong>Filesystem.</strong> The arrays use ext4, 512 KiB RAID chunks and 4 KiB filesystem "
        "blocks; filesystem creation requests 16,000,000 inodes per array. Available bytes and inodes are "
        "checked before each fixture.</li>"
        "<li><strong>Root disk.</strong> The separate encrypted 16 GiB gp3 EBS root disk holds the OS and "
        "tools; it is not the benchmark data disk.</li>" + beside + "</ul>"
        "<p>This is the current configured design, not a substitute for observed host evidence; historical "
        "runs may differ. What the workers recorded is under <a href='#observed'>Observed values</a>.</p></section>"
        "<section aria-labelledby='storage'><h2 id='storage'>What the storage allows</h2>"
        "<p>AWS specifies the four drives together at 1,400,000 random reads and 680,000 writes of 4 KiB a "
        "second, at full queue depth. For an array of two that is about 2.9 GB/s read and 1.4 GB/s written in "
        "requests of that size; larger requests can move somewhat more. This is the provider's specification, "
        "not a measurement of this machine.</p>"
        "<p>" + bound + same + "The copy "
        "duration of these file sets shows whether rcp keeps up with the storage, not how fast rcp could go: a "
        "change in rcp shows first in CPU time and memory. " + unbound + "</p>" + storage + "</section>"
        "<section aria-labelledby='datasets'><h2 id='datasets'>Datasets</h2>"
        "<div class='table-scroll'><table class='data'><thead><tr><th>File set</th><th>Files</th><th>Bytes per file</th>"
        "<th>Total logical data</th></tr></thead>"
        "<tbody>" + sets + more_sets +
        "</tbody></table></div>"
        "<p>Sizes above are decimal bytes. " + sets_note + more_sets_note + " No file-set "
        "definition establishes a completed AWS qualification. Consult each run's suite, outcome and "
        "diagnostics. " + commissioning + " two small fixtures instead "
        "(16 files of 4 MB and 10,000 files of 1 KB) to exercise a run end to end; they are not performance "
        "data and have no trend.</p></section>"
        "<section aria-labelledby='method'><h2 id='method'>Method</h2><ul>"
        + copy + other +
        "<li><strong>Fixtures.</strong> Fixtures are random bytes without a fixed seed, generated with "
        "<code>filegen --leaf-files</code> and a write buffer of min(file size, 1 MiB).</li>"
        "<li><strong>Binaries.</strong> Release x86-64 musl binary source revisions and digests are recorded "
        "for each run.</li>"
        "<li><strong>Repeats.</strong> Runs of a <code>matrix-…</code> suite make three fresh copies " + repeats +
        " with one build, and measure as many file sets as fit their allocation; each run page lists "
        "what was measured and why anything else was not. Runs of the other suites make four copies of each "
        "selected file set, as two balanced adjacent pairs of identical baseline and candidate binaries.</li>"
        "<li><strong>Caches and validation.</strong> " + caches + " Matrix "
        "runs do not verify copies: a copy counts when rcp exits cleanly and reports the expected file "
        "count. Runs of the other suites check paths, types, counts, sizes and metadata, not file contents. "
        "Each run page says which applies.</li>"
        "<li><strong>What is timed.</strong> Generation, cache preparation, validation and deletion are "
        "outside copy wall-time measurement.</li>"
        "<li><strong>Limits.</strong> Commands run as an unprivileged user in an isolated measurement service "
        "with a configured 180 GiB memory ceiling, 8,192 kernel-task ceiling and 1,048,576 open-file limit. "
        "Those are controls, not observed consumption or a bound on queued Tokio futures.</li></ul></section>"
        "<section aria-labelledby='observed'><h2 id='observed'>Observed values</h2>")
    if observations:
        count = len(observations)
        text += ("<p>What the workers recorded, merged across the " + str(count) +
            (" run" if count == 1 else " runs") + " in this snapshot that recorded a setup. Missing "
            "historical fields stay “not recorded”; RAM is not inferred from the instance type.</p>" +
            _observed_html(observations))
    else:
        text += "<p>No observed setup was available in this snapshot.</p>"
    return text + "</section>"


def _delta(metrics, key):
    value = metrics.get(key, {})
    return value.get("delta") if value.get("status") == "available" and number(value.get("delta"), integer=True) else None


def _maximum(metrics, key):
    value = metrics.get(key, {})
    return value.get("maximum") if value.get("status") == "available" and number(value.get("maximum"), integer=True) else None


def _cpu_busy(metrics):
    """The share of all processors' time that was neither idle nor waiting for I/O, or None when not recorded."""
    total, idle, wait = (_delta(metrics, key) for key in
                         ("host_cpu_total_ticks", "host_cpu_idle_ticks", "host_cpu_iowait_ticks"))
    return None if None in (total, idle, wait) or total == 0 or idle + wait > total else (total - idle - wait) / total


def _copy_use(trial, period):
    """What the arrays and the processors did during one copy's command phase; figures it lacks are None.

    A phase's counters run from its first sample to its last, not over the whole phase. Samples are
    one `period` apart, so the counters span one period less than there are samples: nothing when
    there is one sample, and up to a period less than the copy took.
    """
    phase = next((item for item in trial["system"]["phases"] if item["phase"] == "command"), None)
    if phase is None or phase["samples"] < 2:
        return {}
    seconds, metrics = (phase["samples"] - 1) * period, phase["metrics"]
    # sector counters are in 512-byte units. Samples are a few milliseconds more than a period apart, so an array
    # that always had a request in progress would otherwise come out a little above the whole
    rate = lambda key: None if _delta(metrics, key) is None else _delta(metrics, key) * 512 / seconds
    share = lambda key: None if _delta(metrics, key) is None else min(_delta(metrics, key) / 1000 / seconds, 1.0)
    return dict(written=rate("destination_write_sectors"), destination_busy=share("destination_io_milliseconds"),
                read=rate("source_read_sectors"), source_busy=share("source_io_milliseconds"), cpu=_cpu_busy(metrics))


def storage_use(value, record):
    """Summarize what the arrays and the processors did during the copies of each measured file set and scenario.

    A figure is the median over the copies of its cell and is given only when every copy has it: a
    median over the copies that happen to have a figure would pick among the repeats. A cell with no
    figure at all has no row. Rates are bytes a second; the rest are shares of the time they cover.
    """
    sampler = None if value["setup"] is None else value["setup"]["instrumentation"]
    if sampler is None:
        return []
    measured = {(item["case_id"], item.get("scenario", LOCAL)) for item in record["measurements"]}
    cells = {}
    for trial in value["trials"]:
        key = (trial["case_id"], trial.get("scenario", LOCAL))
        if key in measured:
            cells.setdefault(key, []).append(_copy_use(trial, sampler["period_seconds"]))
    rows = []
    for (case_id, scenario), copies in cells.items():
        figures = {}
        for name in ("written", "destination_busy", "read", "source_busy", "cpu"):
            values = sorted(copy[name] for copy in copies if copy.get(name) is not None)
            middle = len(values) // 2
            figures[name] = None if len(values) != len(copies) else (
                values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2)
        if any(figure is not None for figure in figures.values()):
            rows.append(dict(case_id=case_id, scenario=scenario, **figures))
    return rows


# the phases in which the paired harness checks a file set or a copy
_CHECKS = frozenset({"initial_verification", "destination", "metadata", "source"})


def time_use(value, record):
    """Where the time of one run went, in seconds: by kind of work and, for a matrix run, by file set.

    A phase lasts from the moment the harness marks it to its next mark, so the phases of a run cover
    its harness from the first mark to the end and no second is counted twice. `commands` holds the
    commands the run times, by the trend each belongs to: a kind of copy, or a tool whichever copies
    it followed. `around` holds what happens between them: generating a file set, emptying the page
    cache, removing a copy, removing the tree rlink made (`links`), removing a file set and what was
    left of its copies, the checks of the paired harness, and every other step. A file set has its
    commands, its generation and the rest; what belongs to no file set is in the total alone.

    None for a run that marked no phase. A run is not `complete` when a command of it recorded none,
    or the steps outside its commands did not: it took longer than its total.
    """
    tool_cells = _tool_cells(value["schema"])
    order = [case["case_id"] for case in value["cases"]]
    commands, around, cases = {}, {}, {}

    def count(group, kind, seconds, case, part):
        group[kind] = group.get(kind, 0) + seconds
        if case is not None:
            cases.setdefault(case, dict(case_id=case, commands=0, generation=0, rest=0))[part] += seconds

    system = value["system"]
    for phase in system["phases"] if system is not None else ():
        index = phase["case_index"]
        kind = {"generation": "generation", "cleanup": "file_sets"}.get(
            phase["phase"], "checks" if phase["phase"] in _CHECKS else "other")
        count(around, kind, phase["elapsed_seconds"], order[index] if index is not None and index < len(order) else None,
              "generation" if kind == "generation" else "rest")
    for trial in value["trials"]:
        scenario = trial.get("scenario", LOCAL)
        tool = tool_cells[scenario][0] if scenario in tool_cells else None
        for phase in trial["system"]["phases"]:
            name = phase["phase"]
            if name == "command":
                count(commands, tool or scenario, phase["elapsed_seconds"], trial["case_id"], "commands")
                continue
            kind = ("cache" if name == "cache" else ("links" if tool == "rlink" else "copies") if name == "cleanup" else
                    "checks" if name in _CHECKS else "other")
            count(around, kind, phase["elapsed_seconds"], trial["case_id"], "rest")
    if not commands and not around:
        return None
    return dict(total=sum(commands.values()) + sum(around.values()), commands=commands, around=around,
                # the paired harness numbers its file sets as it was given them, not as a page lists them
                cases=[cases[case] for case in order if case in cases] if _matrix(record) else [],
                complete=bool(system is not None and system["phases"]) and
                         all(trial["system"]["phases"] for trial in value["trials"]))


def _unit(value, unit, divisor=1):
    return "unavailable" if value is None else f"{value / divisor:,.3f} {unit}"


def system_html(value):
    if value is None or value["status"] == "unavailable":
        return "<p>Sampled system diagnostics were not recorded or are unavailable.</p>"
    rows = []
    for phase in value["phases"]:
        metrics = phase["metrics"]
        busy = None if _cpu_busy(metrics) is None else 100 * _cpu_busy(metrics)
        available = metrics.get("host_memory_available_bytes", {})
        minimum = available.get("minimum") if available.get("status") == "available" else None
        fields = (
            phase["phase"], str(phase["case_index"]) if phase["case_index"] is not None else "—",
            str(phase["trial_index"]) if phase["trial_index"] is not None else "—",
            phase["status"], f'{phase["elapsed_seconds"]:.3f}',
            _unit(_delta(metrics, "cpu_usage_usec"), "s", 1000000),
            _unit(_maximum(metrics, "memory_current_bytes"), "MiB", 2 ** 20),
            _unit(minimum, "GiB", 2 ** 30),
            "unavailable" if busy is None else f"{busy:.2f}%",
            _unit(None if _delta(metrics, "source_read_sectors") is None else
                _delta(metrics, "source_read_sectors") * 512, "MiB", 2 ** 20),
            _unit(None if _delta(metrics, "destination_write_sectors") is None else
                _delta(metrics, "destination_write_sectors") * 512, "MiB", 2 ** 20))
        rows.append("<tr>" + "".join("<td>" + escape(item) + "</td>" for item in fields) + "</tr>")
    return ("<p>Sampler status: " + escape(value["status"]) + "; samples: " + str(value["samples"]) + ".</p>"
        "<p>CPU busy excludes idle and I/O-wait ticks. Device sector counters use 512-byte sectors. "
        "Indexes are zero-based in recorded case/trial order. Ten-second sampling can miss short phases and boundary activity. Sampled cgroup-current maxima are not exact peaks, and any cgroup lifetime peak may include "
        "earlier phases. “Observed” means a phase ended, not that the benchmark phase succeeded.</p>"
        "<div class='table-scroll'><table class='data dense'><thead><tr><th>Phase</th><th>Case index</th>"
        "<th>Trial index</th><th>Observation</th><th>Elapsed s</th><th>Cgroup CPU</th><th>Sampled cgroup memory max</th>"
        "<th>Host memory available min</th><th>Host CPU busy</th><th>Source logical reads</th>"
        "<th>Destination logical writes</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>")


def _other_storage_html(other):
    """What the storage beside the arrays was in one run, as its worker measured and reported it."""
    text = "<h3 id='other-storage'>Other storage of this run</h3>"
    for kind, what in (("nfs", "The mounts of the network filesystem"), ("memory", "The roots in memory")):
        item = other.get(kind)
        if item is None:
            continue
        if item["status"] != "prepared":
            text += ("<p>" + what + (" could not be prepared for this run; its cells there failed.</p>"
                                     if item["status"] == "unavailable" else " were not recorded for this run.</p>"))
        elif kind == "nfs":
            text += ("<div class='table-scroll'><table class='data'><thead><tr><th>Mounts</th>"
                "<th>Measured round trip</th><th>Added delay</th><th>Server threads</th></tr></thead><tbody><tr>"
                "<td>NFS " + escape(item["version"]) + "</td><td>" + f"{item['round_trip_microseconds'] / 1000:.3f} ms" +
                "</td><td>" + f"{item['delay_microseconds'] / 1000:g} ms" + " on every packet of the loopback</td><td>" +
                f"{item['threads']:,}" + "</td></tr></tbody></table></div>")
        else:
            size = item["size_bytes"]
            text += ("<div class='table-scroll'><table class='data'><thead><tr><th>Roots</th><th>Size limit</th>"
                "<th>File limit</th></tr></thead><tbody><tr><td>two filesystems in memory</td><td>" +
                (f"{size >> 30:,} GiB" if size % (1 << 30) == 0 else f"{size:,} bytes") + " each</td><td>" +
                f"{item['inodes']:,}" + " each</td></tr></tbody></table></div>")
    return text


def _work_progress_html(trials):
    panels = []
    for trial in trials:
        if "work_progress" not in trial:
            continue
        value = trial["work_progress"]
        heading = f'{escape(trial["case_id"])} / {escape(trial["scenario"])} / run {trial["iteration"]}'
        if value["status"] != "observed":
            panels.append(f'<p>{heading} — {escape(value["status"])}</p>')
            continue
        metric = "Files copied over time" if value["metric"] == "files_copied" else "Hard links created over time"
        rows = []
        previous = None
        for stamp, count in value["samples"]:
            rate = "—"
            if previous is not None and stamp > previous[0]:
                rate = f"{1000 * (count - previous[1]) / (stamp - previous[0]):,.1f}"
            rows.append(f"<tr><td>{stamp / 1000:,.3f}</td><td>{count:,}</td><td>{rate}</td></tr>")
            previous = stamp, count
        panels.append(f'<details><summary>{metric} · {heading}</summary><p>Command start '
                      f'{value["command_start_ms"] / 1000:,.3f} s on the system sampler clock. '
                      f'Showing {len(rows)} of {value["observations"]} observations.</p>'
                      '<div class="table-scroll"><table><thead><tr><th>Seconds since command start</th>'
                      '<th>Completed files</th><th>Files/s since previous row</th></tr></thead><tbody>'
                      + "".join(rows) + "</tbody></table></div></details>")
    if not panels:
        return ""
    return ('<details><summary>File progress during commands</summary><p>The archive retains observations '
            'at the reported cadence. Copies count completed data transfers and links count successful hard '
            'links. Remote copies use observations from the destination daemon.</p>' + "".join(panels) + "</details>")


def _nfs_rpc_html(trials):
    rows = []
    for trial in trials:
        if "nfs_rpc" not in trial:
            continue
        window = trial["nfs_rpc"]
        links = window.get("hard_links_created")
        prefix = ("<tr><td><code>" + escape(trial["case_id"]) + "</code></td><td>" + str(trial["iteration"]) +
                  "</td><td>" + ("—" if links is None else f"{links:,}") + "</td>")
        if window["status"] != "available":
            reason = {"mount_changed": "mount changed", "counter_reset": "counter reset",
                      "unavailable": "unavailable"}[window["status"]]
            rows.append(prefix + "<td colspan='7'>" + reason + "</td></tr>")
            continue
        for name in ("All procedures", *nfs_rpc.OPERATIONS):
            counters = window["total"] if name == "All procedures" else window["operations"].get(name)
            row = prefix + "<td>" + name + "</td>"
            if counters is None:
                row += "<td colspan='6'>unavailable</td>"
            else:
                count = counters["completed"]
                fields = [f"{counters[key]:,}" for key in nfs_rpc.COUNTERS]
                fields.extend((f"{counters['rtt_milliseconds'] / count:.3f}" if count else "—",
                               f"{count / links:.3f}" if links else "—"))
                row += "".join("<td>" + item + "</td>" for item in fields)
            rows.append(row + "</tr>")
    if not rows:
        return ""
    return ("<h3 id='nfs-rpc'>NFS client RPCs during rlink</h3>"
            "<p>Linux counts NFSv4.2 RPC procedures on the destination mount between the readings immediately "
            "before and after each command. Other traffic on this mount and session traffic shared with "
            "mounts of the same server can contribute. The total "
            "includes every procedure. A procedure can send several NFSv4 operations in one compound request. "
            "Cumulative round-trip time sums RPC durations, including overlapping requests. RPCs per hard link "
            "divides completed RPCs by the hard links reported by a successful rlink run.</p>"
            "<div class='table-scroll'><table class='data dense'><thead><tr><th>File set</th><th>Run</th>"
            "<th>Hard links created</th><th>Procedure</th><th>Completed RPCs</th><th>Transmissions</th>"
            "<th>Retransmissions</th><th>Cumulative RTT (ms)</th><th>Mean RTT (ms/RPC)</th>"
            "<th>RPCs per hard link</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>")


def render(value, record):
    """Render one run's per-copy resources, then its observed setup as a collapsed appendix."""
    validate(value, record)
    matrix = _matrix(record)
    with_roles = value["schema"] in ROLE_SCHEMAS
    # the tool cells of this document, and with them whether it has any
    tool_cells = _tool_cells(value["schema"])
    tools = bool(tool_cells)
    text = ("<section aria-labelledby='resource-diagnostics'><h2 id='resource-diagnostics'>Resource diagnostics</h2>"
        "<p>Supplementary diagnostics derived from the exact retirement-bound worker archive. The "
        "canonical qualification record, verdict and measurement series are unchanged.</p>"
        "<p>Process-resource availability: " +
        escape(value["status"]) + ". " + escape(value["process_scope"]) + "</p>")
    if value["reason"] == "phase_summary_budget":
        text += ("<p>Optional sampled phase detail exceeded the public document budget and is omitted. "
            "Per-copy process metrics and observed setup remain shown; the bounded raw telemetry "
            "journal remains in private retained evidence.</p>")
    rows = []
    for trial in value["trials"]:
        # one row for each measured process: the copy command or the tool and, for a remote copy, each daemon
        command = tool_cells[trial["scenario"]][0] if trial.get("scenario") in tool_cells else "rcp"
        processes = [(command, trial["metrics"])] if with_roles else [(None, trial["metrics"])]
        if with_roles and trial["roles"] is not None:
            processes += [(role + " rcpd", trial["roles"][role]) for role in ROLES]
        for process, metrics in processes:
            fields = [str(trial["iteration"])] + ([] if process is None else [process])
            if metrics is None:
                fields += ["unavailable"] * (len(METRICS) + 1)
            else:
                fields += ["complete", f'{metrics["user_seconds"]:.3f}', f'{metrics["system_seconds"]:.3f}']
                fields += [f"{metrics[key]:,}" for key in METRICS[2:]]
            rows.append("<tr><td><code>" + escape(trial["case_id"]) + "</code></td><td><code>" +
                        escape(trial["scenario"] if matrix else trial["variant_id"]) + "</code></td>" +
                        "".join("<td>" + escape(item) + "</td>" for item in fields) + "</tr>")
    if rows:
        text += ("<div class='table-scroll'><table class='data dense'><thead><tr>" +
            ("<th>File set</th><th>Scenario</th><th>" + ("Copy or run" if tools else "Copy") + "</th>" +
             ("<th>Process</th>" if with_roles else "")
             if matrix else "<th>Case</th><th>Variant</th><th>Repeat</th>") +
            "<th>Resource status</th><th>User CPU s</th><th>System CPU s</th><th>Peak RSS KiB</th>"
            "<th>Voluntary switches</th><th>Involuntary switches</th><th>Minor faults</th><th>Major faults</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>")
    else:
        text += "<p>No per-copy process-resource measurements are available.</p>"
    if value["schema"] == STORAGE_SCHEMA:
        text += _other_storage_html(value["other_storage"]) + _nfs_rpc_html(value["trials"])
    if value["cases"]:
        progress_rows = []
        for case in value["cases"]:
            stage = ("Copy trials recorded" if case["copy_trials"] else
                "Phase boundary recorded; no copy trial recorded" if case["phase_boundary_recorded"] else
                "No copy trial recorded; generation/attempt status unknown")
            fields = (case["case_id"], case["definition_status"], str(case["copy_trials"]),
                      str(case["successful_copies"]), stage)
            progress_rows.append("<tr>" + "".join("<td>" + escape(item) + "</td>" for item in fields) + "</tr>")
        # how far an unqualified run got is the first thing its reader looks for
        text += ("<details" + ("" if record["benchmark_valid"] else " open") +
            "><summary>Recorded case progress</summary><p>Diagnostic activity only; this table does not qualify "
            "individual cases or turn partial copies into performance samples.</p><div class='table-scroll'>"
            "<table><thead><tr><th>Case</th><th>Definition</th><th>Copy trials recorded</th>"
            "<th>Successful copy outcomes</th><th>Activity evidence</th></tr></thead><tbody>" +
            "".join(progress_rows) + "</tbody></table></div></details>")
    if matrix:
        text += _work_progress_html(value["trials"])
    text += ("<details><summary>Sampled run phases</summary><p>" + escape(SYSTEM_SCOPE) + "</p>" +
             system_html(value["system"]))
    for trial in value["trials"]:
        if trial["system"]["status"] == "unavailable":
            continue
        text += ("<details><summary>Sampled phases: " + escape(trial["case_id"]) + " / " +
            escape(trial["scenario"] if matrix else trial["variant_id"]) +
            (" / run " if trial.get("scenario") in tool_cells else " / copy " if matrix else " / repeat ") +
            str(trial["iteration"]) + "</summary>" +
            system_html(trial["system"]) + "</details>")
    text += ("</details><details><summary>Run limits and source archive</summary><p>Recorded allocation limit: " +
        _display(value["limits"]["allocation_seconds"], " seconds") + "; recorded per-command timeout: " +
        _display(value["limits"]["command_timeout_seconds"], " seconds") + ".</p>")
    if value["archive_sha256"]:
        text += "<p>Source archive SHA256: <code>" + value["archive_sha256"] + "</code>.</p>"
    text += ("</details></section><section aria-labelledby='observed-setup'>"
             "<h2 id='observed-setup'>Observed setup</h2>")
    if value["setup"] is None:
        text += "<p>No observed setup was recorded for this run.</p>"
    else:
        text += ("<details><summary>Hardware, storage and software recorded by this run</summary>" +
                 setup_html(value["setup"]) + "</details>")
    return text + "</section>"
