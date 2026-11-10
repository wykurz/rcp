# Literal-loopback native smoke

The standalone native owner is a functional stepping stone for AWS remote copies. It uses an actual
private IPv4 loopback interface, with both operands addressed as `127.0.0.1`. It is separate from
the existing localhost pull mode and from the veth/netem RTT executor. It does not change either
mode, the AWS worker, admission, schedules, publication, or historical series.

This is **not a performance qualification or a general sandbox for hostile code**. Only use
operator-reviewed rcp/rcpd binaries. A source revision supplied by the operator is a provenance
assertion; it is not a cryptographic build attestation. Binary bytes are copied and SHA-256 checked
through held descriptors, without preserving setuid bits, capabilities, xattrs, or source metadata.
Selected binaries and their protocol probes execute only after the unit has entered its nonroot,
private-network environment.

## Execution and measurement

One fresh systemd service contains the master, both rcpd endpoints, private SSH server and clients,
and the support Python processes. It uses TLS defaults, a private `127.0.0.1:2222` SSH listener,
pinned ephemeral host identity, no agent or proxy, and no external network interface. It never
modifies host SSH configuration or an existing key or account. New disposable SSH keys exist only in
the private fixture; they are unrelated to GitHub App signing keys or user login keys.

The chosen existing account must have a positive UID/GID and no extra host groups. The payload
checks its UID, GID, supplementary groups, capabilities, no-new-privileges setting, private passwd
view and network namespace before starting SSH or the copy. An otherwise unused account such as
`nobody` may fit; availability and actual SSH behavior still require native validation.

| Field                              | Definition                                                                                |
| ---------------------------------- | ----------------------------------------------------------------------------------------- |
| `unit_duration_seconds`            | Unit main-process monotonic lifetime, including SSH setup, probes and teardown            |
| `command_seconds`                  | Separate rcp command interval inside the unit                                             |
| `cpu_total_seconds`                | Fresh copy cgroup user CPU plus system CPU across master, endpoints, SSH and support      |
| `peak_cgroup_charged_memory_bytes` | Fresh copy cgroup hierarchical peak charge, including charged cache/kernel/support memory |

Peak cgroup charge is **not RSS**, a sum of process RSS peaks, or all host memory used by the copy.
Pages already charged outside the cgroup during fixture/binary preparation can remain charged
elsewhere. Fixture and key preparation and content validation run outside the measured unit. Linux
monotonic clocks and their behavior during host/VM suspension also differ from UTC.

The owner retains the exact manager InvocationID and an open descriptor for that cgroup, verifies
recursive `populated=0`, and reads raw `cpu.stat` and `memory.peak` before stop. Missing or invalid
counters fail acceptance. There is no parent-cgroup, manager-cached-peak or process-RSS fallback.
The schema/profile are `rcp-owned-literal-loopback-v1` /
`systemd-private-ipv4-loopback-per-copy-v1`; these records are not imported into local AWS history.

## Explicit native acceptance

Offline tests exercise the control flow and held-file behavior. They do not prove that this host
retains an empty cgroup and its raw counters, private SSH works, or a native copy succeeds. The
native smoke is never started by ordinary CI.

After separate approval for native execution, use a reviewed checkout in an explicit trusted root
context, with systemd, unified cgroup v2 CPU/memory controllers, OpenSSH and Python 3.12 available:

```console
just benchmark-loopback-smoke --run-reviewed-native-smoke \
  --ack-reviewed-binaries --source-commit REVIEWED_SOURCE_SHA \
  --account REVIEWED_EXISTING_NONROOT_ACCOUNT \
  --rcp /absolute/reviewed/rcp --rcp-sha256 RCP_SHA256 \
  --rcpd /absolute/reviewed/rcpd --rcpd-sha256 RCPD_SHA256
```

The owner creates one UUID-named fixture under `/run/rcp-loopback-smoke` and private evidence under
`/var/lib/rcp-loopback-smoke`. It copies only three tiny fixed files, with a 180-second service
runtime limit, 512 MiB memory limit, 256-task limit, and whole-cgroup termination. These are local
smoke bounds, not production benchmark settings or an absolute termination guarantee.

A durable intent precedes the single start. Lost start or stop responses are reconciled without
blind resends. A matching InvocationID is checked before the name-bound stop; systemd does not offer
an atomic InvocationID condition on that action. Concurrent privileged replacement or migration is
outside this cooperative-owner guarantee.

Cleanup requires manager inactivity and proof from the held original cgroup: either recursive
`populated=0`, or kernel removal of that genuine cgroup v2 directory. The kernel permits removal
only without children or live members; zombies can remain, so this does not prove every process was
reaped. This removal proof is for cleanup only and never replaces missing measurement counters. See
the
[kernel cgroup v2 contract](https://docs.kernel.org/admin-guide/cgroup-v2.html#organizing-processes-and-threads).

Only then is the known fixture removed using descriptor-contained, symlink-resistant cleanup.
Evidence remains private. Uncertain ownership, stop or emptiness leaves the fixture and diagnostics
for recovery. Keep private fixture files out of Git, public reports and chat.

```console
just benchmark-loopback-smoke --recover UUID_FROM_PRIVATE_EVIDENCE
```

Recovery adopts only the retained plan and never changes inputs or resends a start/stop intent. A
completed result is historical evidence, not a fresh resource readback. If an uncertain start left
no observable unit/cgroup identity, automated recovery holds rather than guessing; operator
investigation is required. Nothing here authorizes a paid remote AWS qualification.
