# Large workload qualification

These manually selected AWS suites reuse the directory shapes from the
[historical performance report](../../docs/reports/performance.md). Sizes are **decimal bytes**.
They live in `benchmarks/large-cases.json`; the original manifest, default selections and Depot
workflows are unchanged.

| Dispatch suite | Case ID            | Directory widths | Files per leaf | Bytes per file |      Files |     Logical bytes |
| -------------- | ------------------ | ---------------- | -------------: | -------------: | ---------: | ----------------: |
| `bulk-1g`      | `bulk-1g-1tb`      | `[10]`           |            100 |  1,000,000,000 |      1,000 | 1,000,000,000,000 |
| `bulk-100m`    | `bulk-100m-1024gb` | `[10]`           |          1,024 |    100,000,000 |     10,240 | 1,024,000,000,000 |
| `bulk-1m`      | `bulk-1m-1024gb`   | `[10,10,10]`     |          1,024 |      1,000,000 |  1,024,000 | 1,024,000,000,000 |
| `tiny-1k`      | `tiny-1k-10240k`   | `[10,10,10,10]`  |          1,024 |          1,000 | 10,240,000 |    10,240,000,000 |

All four use fresh local copies, existing random-byte `filegen`, baseline/candidate binary pins, two
balanced adjacent pairs (seed 0), disabled coarse timings, full verification and GNU time resources.
They are **smoke diagnostics**, not history-eligible performance runs, even if all copies succeed.
The three bulk payloads exceed the host's 192 GiB RAM. The tiny payload does not; its challenge is
file count and metadata work. No sparse or all-zero substitute is used.

The historical XFS measurements used a same-filesystem layout that could benefit from reflinks.
These suites reuse the shapes, not those storage semantics: source and destination are separate ext4
filesystems. Do not compare their absolute times as an XFS reproduction.

## Storage admission

Only these four suites create two disjoint RAID0 arrays on the four expected approximately 900 GB
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
must report the held array's device number. This operates before candidate execution, on a
disposable guest where no other privileged actor may rearrange storage. It is not protection from a
concurrent root administrator or kernel compromise.

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
1,024. These are conservative estimates, not a guarantee against ENOSPC. Filesystem overhead and
concurrent candidate writes can still exhaust storage; failure must end within the lease. The
per-file process limit is 1 GiB only for these suites, sufficient for a decimal 1 GB file. Legacy
suites retain their 512 MiB limit. Output remains a 128 MiB tmpfs with a 64 MiB archive cap and the
existing bounded collector, object keys and retention.

## Verification and timing

`--streaming-verification` replaces the legacy per-file dictionaries with versioned compact SHA-256
tree, mode and source-metadata digests. The fresh-only walker opens directories and files without
following final-component symlinks, reads every file byte, rejects links/special files, ACLs and
foreign ownership, and checks held-file stability during reads. Source content, paths, modes, owners
and modification times are checked after every successful copy; destination modes are checked
without imposing extra timestamp-preservation semantics.

Memory scales with a bounded directory/depth and worker batch, not total file count. Traversal
allows at most 4,096 entries per directory and depth 128. It uses eight hashing workers by default
(`--verification-workers`, range 1–32), at most twice that many pending file tasks, and 1 MiB read
buffers. Manifest counts and the 1,000,000,000-byte per-file ceiling bound accepted content. The
digest order is deterministic across worker counts. The verifier version and worker count are
included in series identity; old result schemas and old identities remain readable and unchanged.

The memory bound applies to the verifier only. The unchanged `filegen` can queue per-file tasks, and
copy tools retain their own resource behavior; whole-workload memory use still needs qualification.

Capacity and preparation run before `sync`/cache drop. No verification read occurs between cache
drop and the timed copy. Destination and source validation occur after the measured command, before
destination deletion. The source is reused for all four copies.

Runner ID `aws-c5d24-ext4-raid0-stream-v1` and storage IDs `c5d-raid0-ext4-source-v1` /
`c5d-raid0-ext4-destination-v1` prevent pooling with the old single-disk layout. Eight verification
workers are an initial bounded choice, not a demonstrated throughput optimum.

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
3. Propose one **60-minute `bulk-1g` A/A** window with the same reviewed binary SHA in both roles.
   This first exercises beyond-RAM data with only 1,000 files. Build before opening the host. The
   first real run must qualify actual mdadm/mkfs/mount behavior and free capacity; offline tests
   cannot establish those properties on EC2.
4. Preserve partial output if any phase times out. The per-generation/per-copy timeout is 1,200
   seconds; the unchanged absolute lease and three-minute reporting reserve still dominate.
   Generation, formatting, all hashing, deletion and upload count. Do not extend or automatically
   retry a window. A 1 TB source plus four full destination/source validations reads many TB;
   completion within one hour is unproven.
5. Verify physical termination and all ASG capacities zero before considering another case. Review
   phase costs, capacity proof, full validation, isolation, resource collection and A/A balance.
   Propose `bulk-100m`, then `bulk-1m`, then `tiny-1k` separately only when evidence supports their
   bounded runs. Do not dispatch all four as a batch.

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
