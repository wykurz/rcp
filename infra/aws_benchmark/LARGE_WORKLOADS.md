# Large workload qualification

These large-workload profiles reuse the directory shapes from the
[historical performance report](../../docs/reports/performance.md). Sizes are **decimal bytes**.
They live in `benchmarks/large-cases.json`; the original manifest, default selections and Depot
workflows are unchanged.

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
