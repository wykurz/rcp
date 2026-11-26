# Large workload qualification

These large-workload profiles reuse the directory shapes from the
[historical performance report](../../docs/reports/performance.md). Sizes are **decimal bytes**.
They live in `benchmarks/large-cases.json`; the original manifest and default selections are
unchanged. Depot benchmark execution is retired independently of these opt-in profiles.

| Historical full-content suite | Case ID            | Directory widths | Files per leaf | Bytes per file |      Files |     Logical bytes |
| ----------------------------- | ------------------ | ---------------- | -------------: | -------------: | ---------: | ----------------: |
| `bulk-1g`                     | `bulk-1g-1tb`      | `[10]`           |            100 |  1,000,000,000 |      1,000 | 1,000,000,000,000 |
| `bulk-100m`                   | `bulk-100m-1024gb` | `[10]`           |          1,024 |    100,000,000 |     10,240 | 1,024,000,000,000 |
| `bulk-1m`                     | `bulk-1m-1024gb`   | `[10,10,10]`     |          1,024 |      1,000,000 |  1,024,000 | 1,024,000,000,000 |
| `tiny-1k`                     | `tiny-1k-10240k`   | `[10,10,10,10]`  |          1,024 |          1,000 | 10,240,000 |    10,240,000,000 |

These four full-content suites are retained for local correctness diagnostics and reading historical
results. They are **not admitted for new AWS dispatches or operator windows**: the measured 1 TB
hashing cost did not fit the 60-minute lease, and million-file/tiny-file full scans have no
demonstrated bounded runtime. Workflow, controller, operator and worker admission reject them before
launch or execution. Re-enabling a profile needs a separately reviewed workload and runtime
qualification; the deadline must not be extended to accommodate it.

All four use fresh local copies, existing random-byte `filegen`, baseline/candidate binary pins, two
balanced adjacent pairs (seed 0), disabled coarse timings, full verification and GNU time resources.
They are **smoke diagnostics**, not history-eligible performance runs, even if all copies succeed.
The three bulk payloads exceed the host's 192 GiB RAM. The tiny payload does not; its challenge is
file count and metadata work. No sparse or all-zero substitute is used.

The historical XFS measurements used a same-filesystem layout that could benefit from reflinks.
These suites reuse the shapes, not those storage semantics: source and destination are separate ext4
filesystems. Do not compare their absolute times as an XFS reproduction.

## Storage admission

The active `bulk-1g-metadata` suite uses the storage contract retained for the four historical
full-content profiles: two disjoint RAID0 arrays on the four expected approximately 900 GB
instance-store NVMe disks. Each array has two members, a 512 KiB chunk and metadata version 1.2;
source and destination each have roughly 1.8 TB raw capacity. RAID0 has no redundancy. Either member
failing invalidates that disposable filesystem and the measurement.

Before creating either array, the trusted supervisor rejects unexpected NVMe models, anything other
than exactly four instance-store whole disks, duplicate paths/device numbers/serials, partitions,
mounts (including root), active swap, holders, read-only disks and existing signatures. It
cross-checks held block descriptors with sysfs model, serial, size and device numbers. EBS root
devices are excluded by model. Existing named arrays or mount paths are never reused. No wipe,
force-format, array repair or alternate disk fallback is performed.

Both newly created arrays must have the expected level, member identities, chunk, metadata, capacity
and distinct device numbers before the first format. Tools receive inherited held `/proc/self/fd`
paths; ext4 mounting disables pathname canonicalization and external helpers. The mounted filesystem
must report the held array's device number. Storage evidence retains array roles, held device
numbers and membership; harness environment evidence records mountinfo major/minor device numbers
and the kernel's device name. A raw mount source such as `/proc/self/fd/5` is an informational
descriptor alias, not a reusable device path. Actions remain bound to held descriptors. This
operates before candidate execution, on a disposable guest where no other privileged actor may
rearrange storage. It is not protection from a concurrent root administrator or kernel compromise.

Each ext4 filesystem requests 4 KiB blocks and 16,000,000 inodes, with lazy inode-table and journal
initialization disabled. Formatting and all metadata initialization consume the existing lease.
Actual **available**, rather than total, blocks and inodes are checked after mounting, before
generation, and again on the destination before each copy. Unknown or inconsistent counts fail.
Same-filesystem source/destination admission fails.

| Case               | Inodes per fixture, including root | 4 KiB file data blocks, bytes |
| ------------------ | ---------------------------------: | ----------------------------: |
| `bulk-1g-1tb`      |                              1,011 |             1,000,001,536,000 |
| `bulk-100m-1024gb` |                             10,251 |             1,024,039,321,600 |
| `bulk-1m-1024gb`   |                          1,025,111 |             1,027,604,480,000 |
| `tiny-1k-10240k`   |                         10,251,111 |                41,943,040,000 |

Admission adds two blocks per directory, 20% byte headroom and 1 GiB; inode admission adds 20% and
1,024. The local harness exposes independent byte/inode percentage and absolute-reserve overrides
documented in
[benchmarking.md](../../docs/benchmarking.md#bounded-verification-for-large-local-fixtures); the
cloud worker pins the default policy. These are conservative estimates, not a guarantee against
ENOSPC. Filesystem overhead and concurrent candidate writes can still exhaust storage; failure must
end within the lease. The per-file process limit is 1 GiB only for these suites, sufficient for a
decimal 1 GB file. Legacy suites retain their 512 MiB limit. Output remains a 128 MiB tmpfs with a
64 MiB archive cap and the existing bounded collector, object keys and retention.

## Verification and timing

`--streaming-verification` replaces the legacy per-file dictionaries with versioned compact SHA-256
tree, mode and source-metadata digests. The fresh-only walker opens directories and files without
following final-component symlinks. Full-content mode reads every file byte; metadata mode does not.
Both reject links/special files, ACLs and foreign ownership and check held-file stability. Source
content (full-content mode), paths, modes, owners, modification/change times and device/inode
identity are checked after every successful copy; destination modes are checked without imposing
extra timestamp-preservation semantics.

Memory scales with a bounded directory/depth and worker batch, not total file count. Traversal
allows at most 4,096 entries per directory and depth 128. It uses eight hashing workers by default
(`--verification-workers`, range 1–32). Pending large-file tasks default to twice the worker count;
`--verification-batch-depth` tunes that multiplier from 1–8. Read buffers default to 1 MiB;
`--verification-read-bytes` tunes them from 4 KiB–16 MiB. Full-content files up to 128 KiB run
inline by default; `--verification-inline-bytes` independently tunes that threshold from 0 through
1,000,000,000 bytes. This local default leaves decimal 1 MB files eligible for parallel hashing; it
is a heuristic, not measured throughput on the AWS host. One-worker full-content and all metadata
walks run inline, avoiding per-file future scheduling. Cancellation records a pending signal,
cancels queued work and stops active reads cooperatively, draining them before closing descriptors.
Manifest counts and the 1,000,000,000-byte per-file ceiling bound accepted content. The digest order
is deterministic across worker counts and inline thresholds. The verifier policy, worker count,
effective inline threshold, read size and batch depth are included in new series identity; old
result schemas and old identities remain readable and unchanged. Manifest shapes that exceed the
directory-entry or depth bound are rejected before fixture generation.

The memory bound applies to the verifier only. The unchanged `filegen` can queue per-file tasks, and
copy tools retain their own resource behavior; whole-workload memory use still needs qualification.

Capacity and preparation run before `sync`/cache drop. No verification read occurs between cache
drop and the timed copy. Destination and source validation occur after the measured command, before
destination deletion. The source is reused for all four copies.

Runner ID `aws-c5d24-ext4-raid0-stream-v1` and storage IDs `c5d-raid0-ext4-source-v1` /
`c5d-raid0-ext4-destination-v1` prevent pooling with the old single-disk layout. Eight verification
workers are an initial bounded choice, not a demonstrated throughput optimum.

## Performance mode without payload hashing

The separate allowlisted suite **`bulk-1g-metadata`** uses the same `bulk-1g-1tb` random-byte
fixture, two balanced adjacent pairs (four copies), cold-cache policy, per-command timeout, storage
layout and absolute lease. Its purpose is `performance`, with runner
`aws-c5d24-ext4-raid0-metadata-v1` and verification identity `metadata-size-tree-v1` (eight
configured hashing workers, a 1 MiB inline threshold/read size and batch depth 2; metadata walks
themselves run inline). These cloud values are frozen independently of local harness defaults. It
passes `--streaming-verification --verification-mode metadata-size`; the four original suites retain
full-content verification and smoke purpose for historical/local use only.

Only this performance mode omits **both the initial and per-trial full-content scans**. The trusted
baked Python verifier checks expected relative paths, counts, entry types and file sizes; it rejects
missing, extra, truncated, linked or special files. Destination modes and source modes, owners and
modification/change times and device/inode identity must still match. These source checks detect
more mutations but do not prove content integrity. Copy exit status, timeouts, reported transfer
counts and local resource collection remain required. The verifier does not use candidate-produced
`rcmp` or other tools to certify output. A digest fingerprints structure and metadata, **not file
contents**; same-size content corruption can go undetected. Reports explicitly disclose that file
content integrity was not checked. Use the retained full-content mode for separate correctness
checks.

Validation stays outside timed copy intervals, and no validation runs between cache drop and copy.
Source reuse and cache semantics are unchanged. The new policy and runner identity prevent pooling
with historical full-content measurements. AWS inspection accepts only explicitly versioned
historical contracts and reports only recorded fields. Approval of a new run requires the exact
current five-field verification contract and all seven source identity fields; missing settings are
not inferred from current defaults. Performance purpose alone does not publish history: the existing
independent per-attempt approval remains mandatory.

Previously measured generation and per-copy durations project approximately **3,104 seconds** for
generation plus four copies. The previous attempt recorded two copy timings; completion of all four
copies remains unproven. Adding the previously observed 92-second setup and 180-second reporting
reserve models about **56.3 minutes before remaining metadata and cleanup overhead**. This is an
estimate, not demonstrated completion or a guarantee. New `bulk-1g-metadata` admission requires an
exactly **3,600-second window**, at least **3,520 seconds** remaining immediately before requesting
capacity, and at least **3,400 seconds** immediately before worker storage preparation. The worker
floor comprises 3,104 seconds plus the 180-second upload reserve and 116 seconds of remaining-work
headroom; the launch floor adds a 120-second startup allowance. A slow approval, upload, boot or
download fails admission rather than extending the lease. Older recorded windows remain readable but
cannot satisfy new-run admission. All formatting, generation, validation, deletion and upload still
consume the fixed 60-minute window; three minutes remain reserved for stop/collection/upload.
Repeating isolated one-copy runs would repeat fixture setup and generation; an isolated
qualification is not a balanced performance comparison. Preserve partial results and stop at the
original cutoff if any phase exhausts its allowance.

## Qualification prerequisites and first run

This change does not deploy, bake an image, arm a window or launch anything. No IAM, OIDC,
instance-role, SSM, network or history-publishing permission change is needed. Use the existing
[account prerequisites and operator workflow](README.md). The only additional package is `mdadm`;
its monitoring/check services and timers are stopped and masked when installed. Assembly and udev
support remain available. The installer and baked manifest/modules require a **new reviewed AMI**
and a closed-stack update. An old image cannot execute the new suites.

After separate authorization:

1. Build one image from the reviewed commit using the existing temporary `t3a.small` helper and its
   independent 60-minute rollback guard. Retain the previous AMI for rollback; record the new source
   SHA, AMI and snapshot. Do not change quotas or permissions to work around a failure.
2. Keep the group closed while updating the pinned AMI/numeric launch-template version. Verify the
   usual exact-main admission, no active prior instance, suspended replacement paths and cutoff
   behavior. Do not run the old draft debug workflow.
3. For performance qualification, select one **60-minute `bulk-1g-metadata` A/A** window with the
   same reviewed source SHA in both roles. Across every active suite, equal source SHAs cause the
   controller to read each baseline binary once and upload those identical bytes to both roles;
   request admission verifies matching binary digests. Both workflow builds still complete and
   retain provenance, but the candidate build is unused in this A/A case. Different source SHAs
   remain a valid A/B performance comparison, not an A/A qualification. This exercises beyond-RAM
   data with only 1,000 files. Build before opening the host. Qualify actual mdadm/mkfs/mount
   behavior and free capacity; offline tests cannot establish those properties on EC2. The earlier
   four-copy full-content attempt did not finish within 60 minutes; repeating it unchanged is not
   the next experiment.
4. Preserve partial output if any phase times out. The per-generation/per-copy timeout is 1,200
   seconds; the unchanged absolute lease and three-minute reporting reserve still dominate.
   Generation, formatting, validation, deletion and upload count. Do not extend or automatically
   retry a window. Metadata mode reduces verification I/O but completion within one hour remains
   unproven. Any new attempt needs fresh admission and budget accounting.
5. Verify physical termination and all ASG capacities zero before considering another case. Review
   phase costs, capacity proof, the recorded validation mode, isolation, resources and A/A balance.
   Future smaller-file profiles need their own reviewed runtime and correctness contract before
   enabling cloud dispatch. Neither the three disabled bulk full-content profiles nor `tiny-1k`
   should be retried unchanged or dispatched as a batch.

For the current us-east-1 planning example, an AWS price readback on 2026-10-20 showed Linux
On-Demand `c5d.24xlarge` at **$4.608/hour**: one nominal 60-minute worker window is $4.608 compute,
and a two-minute scheduled-action delay adds $0.1536 before asynchronous termination time.
Recheck the selected region's price and current expenditure before approval. Set aside **$7** for
one image build plus this one qualification attempt, including a conservative allowance for builder
compute, root EBS, public IPv4, S3 and snapshot retention. That is a planning reserve, not a
guaranteed bill or permission to spend. Stop and reconcile actual spend if the estimates or
prerequisites change. Never promise a dollar cap from the ASG deadline.

## Offline evidence and remaining limits

`just aws-benchmark-test` and `just benchmark-test` cover exact case arithmetic, inode/block
thresholds and unknown values, unsafe storage inventories, held descriptor commands, failures before
formatting, array membership, capacity failure, streaming content/metadata corruption, bounded
traversal, cold-cache ordering, new worker contracts and legacy parsing. Storage commands are
recorded fakes; fixtures are small temporary trees. Existing cancellation, deadline, collection, IAM
and workflow-policy tests remain applicable.

These tests do not measure million-file performance, prove an hour is sufficient, qualify the Ubuntu
image/tool versions on instance-store hardware, or perform live cutoff/cancellation acceptance.
Those checks belong to a separately approved disposable qualification run.

## Incremental 100 MB metadata qualification

`bulk-100m-metadata` reuses the existing `bulk-100m-1024gb` shape: 10,240 files of 100,000,000
decimal bytes, totaling 1.024 TB. It uses the same separate ext4 filesystems, capacity margins,
metadata-only verifier and two balanced adjacent pairs (four copies) as the 1 GB metadata suite. It
records a distinct suite/case identity; these measurements must not be pooled with the 1 GB case or
historical full-content scans. Only the current complete five-field cloud verification contract is
recognized for this new suite; old 1 GB verification settings are not invented for it.

This is prepared support, not live qualification or permission to spend. The weekly schedule asks
for its one scheduled suite regardless of manual inputs and never selects this profile. The 100 MB
profile stays disabled in legacy 60-minute operator windows. A direct instance-bound attempt must
carry its own exact `bulk-100m-qualification-budget-v1` plan: at least 10,200 seconds left at worker
admission, with 300-second boot and shutdown reserves. This requires the existing maximum
10,800-second allocation. These numbers define an **experimental fixed qualification budget**, not a
measured or predicted completion time. The plan does not copy or scale the 1 GB-derived 3,104-second
projection. Existing per-command timeouts, the absolute deadline, reporting reserve,
single-allocation rule and cleanup horizon remain unchanged; insufficient startup time fails closed
and no deadline is extended.

The production `manual_suites` policy is a separate server-owned opt-in. Its default is exactly
`["bulk-1g-metadata"]`; an exact legacy policy payload without this field retains that same
authority. Unknown or incomplete payloads still fail closed. Even an explicit manual 100 MB opt-in
cannot change the scheduled suite. The manual workflow choice is validated before emitting build
outputs, but the server remains authoritative and rejects an unapproved suite before reservation or
launch.

Before a separately approved paid qualification:

1. Merge reviewed code and prepare compatible controller, collector and reader support. Preserve
   existing 1 GB request bytes, plan versions and frozen reports. Do not change the live server
   policy merely because code now recognizes another suite. Read-only publication support may land
   before qualification, but a new controller package must not be mixed with an old worker image or
   launch-template startup: the exact reviewed worker hash intentionally changes.
2. Build a reviewed worker AMI: `install-worker.sh` bakes the common/worker/harness code, so the old
   image cannot run this contract. Prepare its numeric launch-template version and exact coordinator
   image-pin permission update, preserving rollback versions and all other access. The expected
   design needs no new App, key, role class or AWS service-action category; the concrete image-pin
   change and live deployment still need action-time approval. Between regular slots and only after
   prior ownership is retired, close admission under that approval, perform the coordinated package,
   image and launch-pin update, verify exact compatibility, then restore the approved admission
   state. Keep native cleanup guards intact and do not displace a scheduled run.
3. Approve one manual 100 MB A/A qualification, its exact runtime/budget allowance, narrowly scoped
   `manual_suites` update and cleanup. Use a day other than Monday: the existing daily-one and
   single-owner controls must not be bypassed to squeeze another run beside the regular suite. Keep
   the live scheduled suite unchanged during this stage.
4. Inspect actual generation, four-copy, metadata-walk, cleanup and resource-use evidence. Record
   failures and partial output honestly. Successful completion would qualify this bounded workload;
   it would not establish broad variance, content integrity or small-file scalability.

The remaining `bulk-1m-1024gb` shape has 1,024,000 files and 1.024 TB; `tiny-1k-10240k` has
10,240,000 files and 10.24 GB. They remain disabled for cloud execution. Capacity arithmetic fits
current theoretical array/inode limits, but actual available-space admission remains mandatory. The
generator currently queues per-file tasks before joining them, so million-file memory and runtime
behavior need separate qualification or later bounded-generator work. This queue structure is not
evidence of an observed out-of-memory failure. Do not enable all cases as one batch or infer
small-file readiness from the 100 MB case.

## Full-file-set learning profile

The all-filesets-metadata profile prepares the four existing large shapes together: 1 GB, 100 MB, 1
MB and 1 KB files. They run sequentially on one worker, with one generated source and four balanced
A/A copies per case. Each case is cleaned before the next; array preparation happens once and
capacity is checked for every selected case. Individual 1 MB and 1 KB metadata profiles are also
recognized, but recognition is not live authorization. A manual request without a suite remains the
1 GB one, and a scheduled request asks for the scheduled suite only.

Sizes are decimal bytes. The tiny shape is already 10,240,000 files of 1,000 bytes, totaling 10.24
GB, compared with 1.024 TB for the 100 MB and 1 MB shapes: exactly 100 times less payload. Its
allocated file blocks are about 41.94 GB before directory overhead and capacity margins. Do not
reduce this fixture a second time or confuse it with the historical tiny-10k smoke fixture (10,240
files of 1,024 bytes, about 10 MiB).

The new immutable full-set plan allows an eight-hour allocation, with 300-second boot and shutdown
reserves and a 28,200-second worker admission floor. This is an experimental budget, not a
completion prediction. Generation and copy commands get a two-hour limit; absolute assignment,
service and controller cutoffs still dominate every phase. Twenty generation/copy commands plus
validation and cleanup do not each receive eight hours. Failure must retain completed trials, phase
evidence and explicit incomplete/unattempted cases; partial work is diagnostic, not a successful
full-set performance qualification.

Only this profile has a two-hour capacity-search ceiling (at most 120 attempts), so its formal
reconciliation horizon is at most ten hours and fifteen minutes after admission. The normal
twelve-hour capacity search and three-hour allocation stay unchanged. The existing sixteen-hour
state-machine ceiling is sufficient; no broad orchestration-timeout increase is needed.

The server policy must explicitly allow all-filesets-metadata and set manual_runtime_seconds to
28800. Exact older 17-field and 18-field policies default this new field to zero and gain no
extended runtime. The manual allowlist stays explicit: every suite that may be started by hand is
named in it, at most eight. Even explicitly listed maintainers cannot request the extended profile:
it requires the immutable owner, and either a manual event with this opt-in or a schedule event with
the separate scheduled approval (`scheduled_suite` and `scheduled_runtime_seconds`). Runtime,
capacity and reservation are derived together from this validated policy and immutable suite
contract, not workflow-supplied durations or prices.

The normal guest supervisor starts with its existing three-hour bound. Only root-owned worker code
may request a bounded systemd runtime extension after validating and consuming the exact
instance-bound full-set assignment, before executing candidate code. It uses the documented main-PID
notification protocol and acknowledgement; changing RuntimeMaxSec on an already-running unit is
unsupported. Ordinary requests never extend their supervisor. The independent janitor does not
depend on DynamoDB for expiry. An instance-only runtime-profile tag identifies the reviewed bound,
eight hours (`all-filesets-8h-v1`) or the twelve hours of every file set with the other tools
(`all-filesets-12h-v1`); a missing tag retains three hours and unknown/mismatched values fail
closed. Volume tags keep their original five-field form. The exact coordinator tag permission change
and new AMI/resource pin still need live security approval and target-image acceptance.

The full-set reservation is fixed at $50 and rejects price calculations above that amount, rather
than silently increasing it. The current $4.608/hour price plus sixteen-minute shutdown and existing
ancillary allowance calculates $43.04 for eight hours. A twelve-hour run reserves a fixed $65 by the
same rule, for a calculated $61.47. This is a planning reservation, not a verified bill,
AWS-enforced cost cap or another allowance. Live deployment, updated AMI/tag permissions and
expanded run need one concrete approval bundle before execution. Reuse previously reserved setup
overhead only after checking its remaining allocation. Do not spend on an intermediate image or run
solely to qualify each smaller case.

Periodic telemetry is enabled for the full-set experiment, not silently added to existing scheduled
measurement identities. It samples host CPU/memory/pressure, the measurement cgroup and logical
source/destination device counters every ten seconds, with bounded samples, bytes, duration and
phase transitions. The fixed sampler policy enters the series identity; sample values do not.
Missing data is unavailable, never a zero measurement. Existing GNU-time CPU, RSS, context-switch
and page-fault evidence remains scoped to each local copy command and its waited descendants,
separate from host/cgroup diagnostics. Cgroup I/O can reflect block-layer accounting; it is not
uniquely physical transferred bytes. See the report setup and resource sections for observed
configuration and limitations.

No large-file generation, image build, permission change or worker launch occurs merely by merging
this source. Keep the scheduled suite unchanged during commissioning. After the full-set result,
inspect actual per-phase time, whole-unit memory, storage pressure and costs before choosing routine
timeouts or scheduling the broader suite.

The extended profile needs a finite, server-enforced manual admission expiry in addition to the
explicit runtime opt-in. Operator recovery planning uses the immutable admitted request timestamp,
not GitHub dispatch time; validation, build and runner queues can consume the admission window. The
expiry does not terminate accepted work or extend its native deadlines. If admission is late or
unproven, restore only the exact temporary opt-in with a new policy version and reconcile ownership;
preserve all counters and independent cleanup guards, and never blindly redispatch.

## Performance-only matrix

The `matrix-*` suites run `python3 -m benchmarks.matrix` on the worker instead of the paired
harness. They use the same two RAID0 arrays, capacity checks, cache broker and sandbox as the
profiles above, and differ in what they measure, where a copy's CPU and memory figures come from,
and how they end. The [production runbook](PRODUCTION.md#benchmark-matrix) covers admission,
qualification and rollout.

- **What runs.** One build (the candidate binaries) with rcp's default settings and
  `--summary --output-format=json`. Each cell is one file set copied three times, locally or through
  `rcpd` over loopback; caches are dropped before every copy. The fixture of a file set is generated
  once, its destination is removed after every copy and the fixture after its last cell, so the next
  file set has the space. Nothing is read back: no initial snapshot, no tree walk, no source
  re-validation.
- **Time.** The allocation reaches the runner as one deadline. Each command (generation, cache
  preparation, a copy) gets the same generous 3600-second limit, which exists to catch hangs. A
  command that the end of the allocation cuts short is `interrupted`, the cells after it are
  `not_run`, and neither counts against rcp. The runner keeps two minutes to save its results and
  removes nothing once the allocation has ended: clearing ten million files could outlast the time
  left, and instance storage ends with the instance.
- **Admission floor.** A matrix run uses whatever time it gets, so the worker declines only when
  less than one hour of the allocation is left (`minimum_worker_seconds=3600`), instead of the
  10,200 and 28,200 seconds that the paired profiles need to finish. A slow start costs time, not
  the run. A suite of every file set still runs for exactly the time of its profile, 28,800 seconds
  or 43,200 with the tools: the supervisor extension and the independent expiry tag are bound to
  that profile.
- **Sampling.** The journal of system samples is bounded for the longest run: twelve hours, 5,120
  samples and 16 MiB, where the paired profiles keep eight hours, 4,096 samples and 8 MiB. A record
  names the bounds it was sampled under, and the public companion accepts either set.
- **Credentials.** The instance role's credentials last about six hours, so a longer run renews them
  when it uploads its results. For that the worker has the SDK ask the instance metadata service up
  to five times, where the SDK asks once by default: a single failed read there would otherwise cost
  the run its results, since the archive is not uploaded again after such an error.
- **Stops and errors.** Results are saved after every step. A copy that fails or hangs fails its
  cell only; so does an error nobody foresaw. A stop signal ends the run and keeps the finished
  cells, which still qualify. Command logs are cut to their head and tail as each command ends: the
  worker's output directory is a 128 MiB tmpfs, and one copy that fails for every file must not fill
  it and stop the results of the other cells from being saved.
- **Other tools.** A suite whose name has `tools` also times `rcmp`, `rlink` and `rrm` on each copy
  of the cell that `tools` follows in the name, in that order, before the copy goes: `rrm`'s run is
  what removes it. The worker fetches those three binaries as well, because the suite's plan names
  them. A tool finds what the copy left in memory, since no cache is dropped for it. The tree that
  `rlink` makes is on the destination array beside the copy and is cleared before `rrm` is timed.
  Such a run clears every tree with its own `rrm`, the file sets on the source array included; if
  `rrm` fails at one, `rm -rf` clears the trees from then on, so the copies after it still have
  their space.
- **Other storage.** A suite whose name has `nfs` or `memory` also measures beside the arrays. For
  `nfs` the supervisor exports the two arrays from the kernel's NFS server on the loopback, adds a
  delay to that loopback and mounts them at `/mnt/rcp-benchmark/nfs-source` and `nfs-destination`.
  The measurement unit has a network of its own, so the delay reaches nothing but these mounts. For
  `memory` it mounts two tmpfs at `memory-source` and `memory-destination`, owned by the benchmark
  account. The arrays hold up to six file sets; a file set in memory is not theirs to fit and is
  checked by the runner where it is generated. A preparation that fails costs the cells on that
  storage and no others.
- **Commissioning fixtures.** The `matrix-commission-…` suites use `commission-64m` (16 files of 4
  MB) and `commission-10k` (10,000 files of 1 KB in 100 directories). They exist to exercise every
  step in seconds and are not performance evidence.
- **Weekly suite.** `matrix-weekly-1` is the one suite that is not every file set of a selection
  under every scenario of a set. Its file sets are sized so that a measured command takes one to
  five minutes: 400 GB of 1 GB, of 100 MB and of 1 MB files (`bulk-1g-400gb`, `bulk-100m-400gb`,
  `bulk-1m-400gb`), and 4,096,000 files of 1 KB on the arrays and in memory (`tiny-1k-4096k`,
  `tiny-1k-4096k-memory`), with the file set on NFS as it is. Only the sets of 1 KB files have tool
  cells. It runs as any suite with the tools and with cells on other storage does, in the eight-hour
  allocation. [The weekly suite](PRODUCTION.md#the-weekly-suite) says how it was sized.

- **Remote cells.** A suite whose name has `remote` runs
  `rcp --force-remote --bind-ip=127.0.0.1 … 127.0.0.1:SOURCE 127.0.0.1:DESTINATION` for those cells.
  The runner starts one unprivileged `sshd` on `127.0.0.1` inside the measurement unit and stops it
  with the run; its keys, made for the run, stay in the unit's private `/tmp` and are never part of
  the archived output, while `logs/sshd.log` is. Such a suite's unit runs with
  `RestrictAddressFamilies=AF_UNIX AF_INET`; `PrivateNetwork=yes` is unchanged, so the sockets reach
  the unit's own loopback and nothing else. The explicit addresses matter: rcp's address detection
  enumerates interfaces over netlink, which the unit does not allow. After every remote copy the
  runner ends any daemon still running, since `sshd` starts daemons outside the copy's process
  group. [Remote cells](../../docs/benchmarking.md#remote-cells) has the details.

The per-copy CPU, memory and sampled phases are published as before. A matrix run records what rcp
reports about itself: the worker asks every copy for its summary as JSON (`--output-format=json`)
and no CPU or memory figure is measured from outside. For a local copy that is the one `rcp`
process. For a remote copy it is three sets of figures: the `rcp` command, the source `rcpd` and the
destination `rcpd`. A revision from before rcp had `--output-format` is still timed, from its text
summary, and has no CPU or memory figures.
