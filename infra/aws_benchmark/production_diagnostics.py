"""Versioned, bounded public diagnostics derived from retirement-bound worker bytes.

These fields supplement an unchanged frozen qualification record. They never
qualify a run, change series IDs, publish raw logs, or execute archive contents.
"""

import math
from html import escape
import re

from .common import digest, require
from .json_bytes import encoded

SCHEMA = "rcp-public-resource-diagnostics-v1"
MAX_DOCUMENT = 512 * 1024
MAX_TRIALS = 128
MAX_NUMBER = (1 << 63) - 1
PROCESS_SCOPE = ("One local copy command, its threads and waited descendants. CPU seconds may exceed "
                 "wall time; peak RSS is a per-command peak, not host memory or a sum of processes.")
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


def _new_setup_value(value, key):
    field = value.get(key, {}) if isinstance(value, dict) else {}
    return field.get("value") if isinstance(field, dict) and field.get("status") == "available" else None


def _side(environment, worker, side):
    filesystem = environment.get("filesystem", {}).get(side, {})
    filesystem = filesystem if isinstance(filesystem, dict) else {}
    storage = worker.get("storage", {})
    storage = storage if isinstance(storage, dict) else {}
    array = storage.get("arrays", {}).get(side, {})
    array = array if isinstance(array, dict) else {}
    capacity = storage.get("capacity", {}).get(side, {})
    capacity = capacity if isinstance(capacity, dict) else {}
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


def setup(raw, worker):
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
            context["verification"].get("policy") in {"metadata-size-tree-v1", "streaming-content-tree-v1"} else None,
        pairing_policy="adjacent-balanced-sha256-v1" if isinstance(context.get("pairing"), dict) and
            context["pairing"].get("policy") == "adjacent-balanced-sha256-v1" else None,
        instrumentation=TELEMETRY_DESCRIPTOR if context.get("system_telemetry") == TELEMETRY_DESCRIPTOR else None)
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
            value["verification_policy"] in {None, "metadata-size-tree-v1", "streaming-content-tree-v1"} and
            value["pairing_policy"] in {None, "adjacent-balanced-sha256-v1"} and
            value["instrumentation"] in (None, TELEMETRY_DESCRIPTOR), "invalid public setup policy")
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
            not number(value.get("samples"), integer=True, maximum=4096):
        return unavailable
    phases = value.get("phases")
    if not isinstance(phases, list) or len(phases) > 192:
        return unavailable
    output = []
    for phase in phases:
        if not isinstance(phase, dict) or phase.get("phase") not in PHASES or \
                phase.get("status") not in {"observed", "running", "interrupted"} or \
                not number(phase.get("elapsed_seconds"), maximum=8 * 3600) or \
                not number(phase.get("samples"), integer=True, maximum=4096):
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


def unavailable(record, reason="not_recorded"):
    require(reason in REASONS, "invalid diagnostic availability reason")
    value = dict(schema=SCHEMA, run_key=record["run_key"], retirement_sha256=record["retirement_sha256"],
        archive_sha256=None, status="unavailable", reason=reason, process_scope=PROCESS_SCOPE,
        system_scope=SYSTEM_SCOPE, setup=None, setup_id=None, trials=[], cases=[], system=None,
        limits=dict(allocation_seconds=None, command_timeout_seconds=None))
    return validate(value, record)




def from_archive(record, archive):
    """Read bounded JSON members only; the caller authenticates archive bytes first."""
    from . import results
    selected, _, _ = results.members(archive)
    raw = results.json_value(selected[results.RESULT], results.MAX_JSON)
    worker = results.json_value(selected["worker.json"])
    return derive(record, archive, raw, worker)

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
        expected = CASE_SHAPES[key]
        definition_indexes[key] = original_index
        strict_types = (type(item.get("files_per_leaf")) is int and type(item.get("file_size_bytes")) is int
            and isinstance(item.get("directory_widths"), list)
            and all(type(width) is int for width in item["directory_widths"]))
        definition_status[key] = ("shape-unavailable" if not set(expected) <= set(item) else
            "recorded" if strict_types and all(item[field] == value for field, value in expected.items()) else "shape-mismatch")
    for trial in selected:
        if trial["case_id"] not in order:
            order.append(trial["case_id"])
            definition_status[trial["case_id"]] = "observed-only"
    phases = []
    context = raw.get("context")
    sampled = isinstance(context, dict) and context.get("system_telemetry") == TELEMETRY_DESCRIPTOR
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
    trials = raw.get("trials")
    if not isinstance(trials, list) or len(trials) > MAX_TRIALS:
        return unavailable(record, "invalid_diagnostics")
    # matching frozen samples prevents a companion from changing historical timing identity.
    grouped = {}
    selected = []
    context = raw.get("context", {})
    sampled = isinstance(context, dict) and context.get("system_telemetry") == TELEMETRY_DESCRIPTOR
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
    observed = setup(raw, worker)
    window = worker.get("window", {})
    window = window if isinstance(window, dict) else {}
    limits = dict(allocation_seconds=_integer(window.get("runtime_seconds"), 8 * 3600),
                  command_timeout_seconds=_integer(worker.get("command_timeout_seconds"), 2 * 3600))
    value = dict(schema=SCHEMA, run_key=record["run_key"], retirement_sha256=record["retirement_sha256"],
        archive_sha256=digest(archive), status="complete" if selected and all(x["metrics"] is not None for x in selected)
            else "partial" if selected else "unavailable",
        reason=None if selected else "not_recorded", process_scope=PROCESS_SCOPE, system_scope=SYSTEM_SCOPE,
        setup=observed, setup_id=setup_id(observed), trials=selected, cases=case_progress(raw, selected),
        system=_system(raw.get("system_telemetry") if sampled else None), limits=limits)
    if len(encoded(value)) > MAX_DOCUMENT:
        # Sampled detail is optional; it must not erase already recorded process metrics/setup.
        value["reason"] = "phase_summary_budget"
        value["system"] = None
        for trial in value["trials"]:
            trial["system"] = _system(None)
    return validate(value, record)


def validate(value, record):
    require(isinstance(value, dict) and set(value) == {"schema", "run_key", "retirement_sha256",
        "archive_sha256", "status", "reason", "process_scope", "system_scope", "setup", "setup_id", "trials", "cases", "system", "limits"} and
        value["schema"] == SCHEMA and value["run_key"] == record["run_key"] and
        value["retirement_sha256"] == record["retirement_sha256"] and value["status"] in STATUSES and
        value["process_scope"] == PROCESS_SCOPE and value["system_scope"] == SYSTEM_SCOPE,
        "invalid public diagnostic identity")
    require(value["reason"] is None or value["reason"] in REASONS, "invalid diagnostic reason")
    require(isinstance(value["limits"], dict) and set(value["limits"]) ==
            {"allocation_seconds", "command_timeout_seconds"} and
            (value["limits"]["allocation_seconds"] is None or
                number(value["limits"]["allocation_seconds"], integer=True, maximum=8 * 3600)) and
            (value["limits"]["command_timeout_seconds"] is None or
                number(value["limits"]["command_timeout_seconds"], integer=True, maximum=2 * 3600)),
            "invalid diagnostic execution limits")
    require(value["archive_sha256"] is None or isinstance(value["archive_sha256"], str) and
        re.fullmatch(r"[0-9a-f]{64}", value["archive_sha256"]), "invalid diagnostic archive digest")
    require((value["setup"] is None) == (value["setup_id"] is None), "invalid diagnostic setup")
    if value["setup"] is not None:
        require(setup_id(value["setup"]) == value["setup_id"], "diagnostic setup fingerprint differs")
    require(isinstance(value["cases"], list) and len(value["cases"]) <= 4, "too many diagnostic cases")
    case_ids = set()
    for case in value["cases"]:
        require(isinstance(case, dict) and set(case) == {"case_id", "definition_status", "copy_trials",
            "successful_copies", "phase_boundary_recorded"} and case["case_id"] in CASE_SHAPES and
            case["case_id"] not in case_ids and case["definition_status"] in {
                "recorded", "shape-unavailable", "shape-mismatch", "observed-only"} and
            number(case["copy_trials"], integer=True, maximum=MAX_TRIALS) and
            number(case["successful_copies"], integer=True, maximum=case["copy_trials"]) and
            type(case["phase_boundary_recorded"]) is bool, "invalid diagnostic case progress")
        case_ids.add(case["case_id"])
    require(isinstance(value["trials"], list) and len(value["trials"]) <= MAX_TRIALS, "too many public resource trials")
    seen = set()
    for trial in value["trials"]:
        require(isinstance(trial, dict) and set(trial) == {"case_id", "variant_id", "iteration", "status", "metrics", "system"} and
                isinstance(trial["case_id"], str) and trial["case_id"] in case_ids and
                trial["variant_id"] in {"rcp-default", "rcp-baseline"} and
                number(trial["iteration"], integer=True, maximum=100) and trial["iteration"] > 0 and
                trial["status"] in {"complete", "unavailable"}, "invalid public resource trial")
        key = (trial["case_id"], trial["variant_id"], trial["iteration"])
        require(key not in seen, "duplicate public resource trial")
        seen.add(key)
        metrics = trial["metrics"]
        require((metrics is None) == (trial["status"] == "unavailable"), "invalid resource availability")
        if metrics is not None:
            require(isinstance(metrics, dict) and set(metrics) == set(METRICS) and
                all(number(metrics[key], integer=key not in {"user_seconds", "system_seconds"}) for key in METRICS),
                "invalid public process metrics")
        require(_system(trial["system"], public=True) == trial["system"], "invalid public trial system summary")
    for case in value["cases"]:
        matching = [trial for trial in value["trials"] if trial["case_id"] == case["case_id"]]
        require(len(matching) == case["copy_trials"] and
                sum(trial["status"] == "complete" for trial in matching) <= case["successful_copies"],
                "case activity differs from resource trials")
    expected_status = "complete" if value["trials"] and all(x["metrics"] is not None for x in value["trials"]) else (
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
        ("Method", "Verification", value["verification_policy"], "software"),
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


def configuration_html(observations):
    """Describe the shared configured design once, then the observed values merged across runs.

    Each observation is dict(run=label, href=run page, setup=observed setup), oldest first.
    """
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
        "tools; it is not the benchmark data disk.</li></ul>"
        "<p>This is the current configured design, not a substitute for observed host evidence; historical "
        "runs may differ. What the workers recorded is under <a href='#observed'>Observed values</a>.</p></section>"
        "<section aria-labelledby='datasets'><h2 id='datasets'>Datasets</h2>"
        "<div class='table-scroll'><table class='data'><thead><tr><th>File set</th><th>Files</th><th>Bytes per file</th>"
        "<th>Total logical data</th></tr></thead>"
        "<tbody><tr><td>1 GB</td><td>1,000</td><td>1,000,000,000</td><td>1 TB</td></tr>"
        "<tr><td>100 MB</td><td>10,240</td><td>100,000,000</td><td>1.024 TB</td></tr>"
        "<tr><td>1 MB</td><td>1,024,000</td><td>1,000,000</td><td>1.024 TB</td></tr>"
        "<tr><td>1 KB</td><td>10,240,000</td><td>1,000</td><td>10.24 GB</td></tr></tbody></table></div>"
        "<p>Sizes above are decimal bytes. The 1 KB file set has exactly 100 times less total logical "
        "data than the 1.024 TB sets; it still exercises over ten million files. No file-set "
        "definition establishes a completed AWS qualification. Consult each run's suite, outcome and "
        "diagnostics.</p></section>"
        "<section aria-labelledby='method'><h2 id='method'>Method</h2><ul>"
        "<li><strong>Copy.</strong> The copy datapath is local on one host, not a network copy; package and "
        "evidence transfers happen outside copy measurement. RCP uses its version-specific default settings "
        "apart from summary output, with no supplied bandwidth or operations throttle.</li>"
        "<li><strong>Fixtures.</strong> Fixtures are random bytes without a fixed seed, generated with "
        "<code>filegen --leaf-files</code> and a write buffer of min(file size, 1 MiB).</li>"
        "<li><strong>Binaries.</strong> Release x86-64 musl binary source revisions and digests are recorded "
        "for each run.</li>"
        "<li><strong>Repeats.</strong> Each selected file set uses two balanced adjacent A/A pairs: four "
        "fresh local copies with identical baseline and candidate binary digests.</li>"
        "<li><strong>Caches and validation.</strong> Linux cache dropping occurs before copies; validation "
        "checks paths, types, counts, sizes and metadata, not file contents.</li>"
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


def _unit(value, unit, divisor=1):
    return "unavailable" if value is None else f"{value / divisor:,.3f} {unit}"


def system_html(value):
    if value is None or value["status"] == "unavailable":
        return "<p>Sampled system diagnostics were not recorded or are unavailable.</p>"
    rows = []
    for phase in value["phases"]:
        metrics = phase["metrics"]
        total, idle, wait = (_delta(metrics, key) for key in
                            ("host_cpu_total_ticks", "host_cpu_idle_ticks", "host_cpu_iowait_ticks"))
        busy = None if None in (total, idle, wait) or total == 0 or idle + wait > total else 100 * (total - idle - wait) / total
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
        "<div class='table-scroll'><table class='data'><thead><tr><th>Phase</th><th>Case index</th><th>Trial index</th>"
        "<th>Observation</th><th>Elapsed s</th><th>Cgroup CPU</th><th>Sampled cgroup memory max</th>"
        "<th>Host memory available min</th><th>Host CPU busy</th><th>Source logical reads</th>"
        "<th>Destination logical writes</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>")


def render(value, record):
    """Render one run's per-copy resources, then its observed setup as a collapsed appendix."""
    validate(value, record)
    text = ("<section aria-labelledby='resource-diagnostics'><h2 id='resource-diagnostics'>Resource diagnostics</h2>"
        "<p>Supplementary diagnostics derived from the exact retirement-bound worker archive. The "
        "canonical qualification record, verdict and measurement series are unchanged. Measurements "
        "remain worker-reported, not independently attested.</p><p>Process-resource availability: " +
        escape(value["status"]) + ". " + escape(PROCESS_SCOPE) + "</p>")
    if value["reason"] == "phase_summary_budget":
        text += ("<p>Optional sampled phase detail exceeded the public document budget and is omitted. "
            "Per-copy process metrics and observed setup remain shown; the bounded raw telemetry "
            "journal remains in private retained evidence.</p>")
    rows = []
    for trial in value["trials"]:
        metrics = trial["metrics"]
        fields = [str(trial["iteration"]), trial["status"]]
        if metrics is None:
            fields += ["unavailable"] * len(METRICS)
        else:
            fields += [f'{metrics["user_seconds"]:.3f}', f'{metrics["system_seconds"]:.3f}']
            fields += [f"{metrics[key]:,}" for key in METRICS[2:]]
        rows.append("<tr><td><code>" + escape(trial["case_id"]) + "</code></td><td><code>" +
                    escape(trial["variant_id"]) + "</code></td>" +
                    "".join("<td>" + escape(item) + "</td>" for item in fields) + "</tr>")
    if rows:
        text += ("<div class='table-scroll'><table class='data'><thead><tr><th>Case</th><th>Variant</th><th>Repeat</th>"
            "<th>Resource status</th><th>User CPU s</th><th>System CPU s</th><th>Peak RSS KiB</th>"
            "<th>Voluntary switches</th><th>Involuntary switches</th><th>Minor faults</th><th>Major faults</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>")
    else:
        text += "<p>No per-copy process-resource measurements are available.</p>"
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
    text += ("<details><summary>Sampled run phases</summary><p>" + escape(SYSTEM_SCOPE) + "</p>" +
             system_html(value["system"]))
    for trial in value["trials"]:
        if trial["system"]["status"] == "unavailable":
            continue
        text += ("<details><summary>Sampled phases: " + escape(trial["case_id"]) + " / " +
            escape(trial["variant_id"]) + " / repeat " + str(trial["iteration"]) + "</summary>" +
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
