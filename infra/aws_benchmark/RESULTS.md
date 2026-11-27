# AWS result ingestion and reporting

The manually dispatched AWS workflow retains a report as well as raw evidence. Reporting is
credential-free and has no infrastructure deployment, AMI rebuild, benchmark launch or history
publishing step. Depot continues to supply its existing smoke, performance, history and Pages jobs
while AWS coverage is evaluated separately.

## Read a run

The trusted controller uploads `aws-benchmark-results`: `status.json`, `results.zip` when available,
and a separate `controller.json` receipt. A fresh GitHub-hosted job downloads the exact artifact ID
returned by that upload step, validates it, adds a job summary and uploads
`aws-benchmark-report-<reporting-attempt>`. Both result artifacts have 90-day retention, subject to
the repository's retention limit. Build artifacts remain at one day. Mailbox payload, transient
control and result objects expire after seven days; the exact `control/mailbox-owner.json` exclusion
marker persists until deliberate, conditional [operator retirement](MAILBOX_RETIREMENT.md).
Reporting adds no AWS storage resources. Cancellation can prevent the controller upload or report
job from running; these are best-effort diagnostics, not the shutdown safety boundary.

Use the artifact ID, SHA-256 digest and direct download link from the trusted `report` job's
summary, then open `index.html` locally. The trusted `benchmark` job similarly identifies its raw
upload. Check that job's upload succeeded; an absent ID means no trusted upload exists. Do not
select an artifact by name, or use a candidate build job's summary as the authority. Candidate code
can create a colliding name before a trusted upload; that collision can leave a forged named
artifact alongside a failed trusted upload. Names, including the reporting-attempt suffix, establish
no provenance. The suffix only lets report-only retries upload without colliding with prior reports.

The report consumes the original benchmark producer's identity output. To retry only reporting,
choose **Re-run job** on the `report` job itself, even if the `benchmark` job also failed. Never
choose **Re-run failed jobs** or **Re-run all jobs** for this purpose: those can rerun the
benchmark. A report-only retry retains the original producer identity, conclusion and artifact ID
even when its own GitHub run-attempt number increases. It needs no window and does not join the
benchmark job's mailbox concurrency group. A new benchmark attempt instead requires a fresh dispatch
under the [operator checklist](README.md#run-one-window).

The report directory contains the existing standalone measurement dashboard at
`dashboard/index.html`, plus:

- `raw/`: byte-for-byte copies of the bounded input files, without extracting `results.zip`.
- `provenance.json`: validation status, producer conclusion, controller/worker observations and
  archive/member SHA-256 fingerprints.
- `summary.md`: generated provenance and median/repeat tables. Archived `summary.md` is never used
  as trusted Markdown or HTML.
- `derived.json`: a separate validated dashboard record, when measurement data is available. Its
  deterministic AWS run identity replaces the worker UUID only in this derived copy; the original
  UUID and revision remain in provenance. Diagnostic records have purpose `diagnostic`, excluding
  them from performance trends and historical change comparisons. An inconsistent reserved
  `approved candidate` role refuses the measurement record, retaining controller/worker evidence but
  producing no derived record or dashboard. Internally consistent candidate-supplied approval claims
  are replaced with the ingester's own outcome; the unchanged raw revision remains in provenance
  when measurement validation succeeds.

Missing, malformed, oversize or digest-mismatched input produces a diagnostic report and a failed
ingestion step. Available bounded raw files are retained. A valid controller receipt is recorded
independently of missing status/archive files, preserving observed image and cleanup information on
failure paths. Status bound to that receipt is also retained before archive completeness checks; its
state, success and bounded error details distinguish upload, protocol and archive-download failures.
After structural archive and digest validation, supervisor metadata remains available even when
candidate-owned member names or measurement JSON exceed the reporting policy. Oversize or special
files are not copied. Worker failure can still produce a valid partial dashboard; it never implies a
successful comparison. Producer cancellation overrides any apparently successful worker record for
history eligibility. Unknown observations remain unknown. Historical controller observations with a
null image ID or instance type retain their worker evidence and measurement dashboard, but cannot
qualify without the observed identity. Expected image pins never replace missing observations.
Durable timing labels must be bounded, printable text or null; historical wording is retained, while
current qualification requires the protocol's clock origin and explicit projected-runtime label.

## Provenance and trust

These revisions have different meanings:

| Field                            | Source and meaning                                                                                                                                                                                                                                                                                          |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Candidate and baseline           | Pinned dispatch inputs, bound to the controller request and its binary hashes. They identify the requested builds.                                                                                                                                                                                          |
| Controller and workflow SHA      | GitHub's exact dispatch checkout/workflow identity. Admission requires literal `refs/heads/main`, the expected repository/workflow, and `workflow_dispatch`.                                                                                                                                                |
| Controller observed AMI          | `ImageId` and instance type observed through the already permitted `ec2:DescribeInstances`. Empty observations retry within the existing deadline; three read errors cap optional reads, with a final observation after mandatory archive retrieval when the deadline budget permits. Missing data is null. |
| Supervisor reported baked source | `worker.json.harness_revision`, read by the root supervisor from the baked `revision.txt`. This is not an independently queried EC2 image tag or the candidate SHA.                                                                                                                                         |
| Raw harness checkout             | The unchanged measurement record's `revision`. The baked tree normally has no `.git`, so this is null. It must not be relabeled as the candidate.                                                                                                                                                           |

The receipt also records the operator window, binary request hashes, downloaded status/archive
hashes, controller outcome and cleanup verification. It is written before launch and updated on
completion; receipt write failures after launch cannot bypass cleanup. An incomplete receipt never
proves teardown. The independent scheduled cutoff is unchanged, can execute up to two minutes late,
and termination remains asynchronous; the report makes no exact billing-cap claim.

The parser accepts only the worker's single-disk, stored, comment-free ZIP format. It checks the
central directory count and member-name byte lengths before allocating ZIP entries: at most 1001
members, 2048 bytes per member name, and 64 MiB total archive and expanded data. It rejects
ZIP64/extra fields, encryption, overlap, hidden/trailing records, duplicate exact paths, traversal
paths, symlinks/special files and CRC failures. Non-ASCII/control characters, colons, case-only
aliases and reporting name limits in candidate-owned `benchmark/` members make measurements
unavailable while preserving validated `worker.json` and member hashes. The worker omits individual
paths it cannot safely collect, records bounded omission reasons, and continues collecting valid
files; omitted contents have no member hash. Historical archives within the structural name bound
still retain hashes for nonportable names. The parser never extracts or executes a member.
Worker/status/receipt JSON is at most 1 MiB; measurement JSON is at most 8 MiB. An oversized
measurement also preserves supervisor evidence; malformed archive structure remains fatal. Strict
JSON rejects duplicate keys/nonfinite numbers and limits depth to 40, nodes to 100000 and strings to
16384 characters. Measurement collections are bounded to 16 cases, 16 variants, 512 trials and 256
summaries before the existing schema/proof validation runs. These are offline reporting limits,
comfortably above the currently allowlisted one-case suites. Diagnostics preserve fixed,
stage-specific parser reasons; errors from generic parsers never echo untrusted input. Invalid
trusted identities/conclusions are rejected before creating an output directory. Generated summaries
use HTML table cells with escaped text and visible control escapes; each displayed value is capped
after control expansion so even maximal supported reports fit GitHub's 1 MiB step-summary limit. The
landing page embeds the same fragment without decoding it again. Change reports and browser
dashboard labels also show control and bidi characters visibly, while the retained measurement data
stays unchanged.

Candidate binaries share the unprivileged harness UID and can forge candidate-owned measurement
files. Hashes and validation establish integrity and traceability, **not honest-measurement
attestation**. Root supervisor metadata and controller observations are separate evidence. The
durable collector cross-checks the worker request, admission, exit status, result schema, proofs and
tool hashes against the pinned contract. These are consistency checks, not attestation that the
candidate-controlled measurements or their reported proofs are honest. The collector's offline
validation runs after AWS credentials are removed; the legacy ASG controller does not parse the ZIP.
The report job has only `contents: read`: no protected environment, OIDC, AWS credentials, history
write token, candidate checkout or candidate execution. The report input is bound to the trusted
producer upload ID and fails closed on input substitution; there is no fallback to names, newest
artifacts, another workflow run or caller-supplied artifact IDs.

The cost boundary remains manual dispatch, reviewed exact main, the protected environment, explicit
operator arming and independently scheduled closure. Reporting introduces no paid-launch trigger,
`workflow_run`, `pull_request_target` or `repository_dispatch` path. An attacker can consume normal
GitHub queue capacity if repository permissions permit dispatch, but cannot open an ASG window. The
environment OIDC subject is not workflow-specific: administrators must protect main and review all
workflows able to use that environment. Any temporary external debug-branch exception is a separate,
coordinated permission decision; this change neither edits nor relies on that exception.

## Offline ingestion and future history staging

No extra AWS information, credentials or account changes are needed for offline reporting. Worker
code updates follow the [AMI update rule](README.md#ami-updates); updating offline reporting alone
does not update or require a worker image.

To inspect downloaded evidence offline, copy the complete JSON from the trusted `benchmark` job's
**Bind the producer attempt before reading candidate artifacts** step: its log prints
`RCP_AWS_PRODUCER_IDENTITY=...`, and its summary includes the same JSON. Save the JSON object
without the prefix as `trusted-identity.json`, and supply that producer job's actual conclusion. Do
not use the validation job's dispatch identity: if the benchmark job was rerun, it describes an
earlier attempt. A later report-only retry still uses the identity of the benchmark attempt that
uploaded the raw artifact. Obtain the identity independently from that trusted job, never from
downloaded candidate evidence:

```bash
just aws-benchmark-report downloaded-raw --identity trusted-identity.json \
  --conclusion cancelled --output new-report-directory
```

Each output directory must be new, preventing stale success pages after failed retries. The input
and output prefixes must be operator-owned and kept out of concurrent untrusted write control. This
command needs Python and repository code only; it makes no network calls. Offline ingestion checks
receipt shape, chronology and identity/hash bindings, without reapplying the checkout's current
window-duration or binary-admission limits to historical receipts. Admission limits are enforced by
the operator, controller and worker when the benchmark runs.

The legacy history-admission path below is an offline gate and local staging operation.
`benchmarks.publish` is a storage helper, not a provenance gate; never feed it arbitrary AWS worker
files or a diagnostic record. The separate [production publisher](PRODUCTION.md) prepares a bounded
retired-evidence pipeline and public pages, initially disabled. Its history-writer authorization and
live acceptance gates require separate approval before activation. Both paths must disclose that
candidate-controlled measurements are self-reported and unattested; consistency and cleanup checks
do not prove measurement honesty.

For an offline staging test, an independently reviewed per-attempt policy has exactly these fields:

```json
{
  "schema_version": 1,
  "identity": { "...": "the complete independently verified run identity" },
  "archive_sha256": "the independently verified results.zip SHA256",
  "ami_id": "the independently approved observed AMI ID",
  "harness_revision": "the approved 40-character baked source SHA"
}
```

`identity` is the full object from the trusted benchmark producer step, including repository,
workflow/ref, run ID-attempt, run URL, event, controller/workflow SHA, suite and candidate/baseline
SHAs. The approver must separately establish that both build revisions are approved for performance
history and that the AMI/baked source pair is trusted. Do not generate this policy from downloaded
evidence, accept it from a candidate PR, or treat a policy boolean as authentication. The policy
pins one exact attempt and archive, preventing substitution of an otherwise valid earlier bundle.

```bash
just aws-benchmark-report downloaded-raw --identity trusted-identity.json \
  --conclusion success --policy independent-policy.json \
  --history local-staging --output new-approved-report-directory
```

Staging additionally requires successful producer/controller/supervisor outcomes, verified cleanup,
observed `c5d.24xlarge`, matching image/baked source/binary digests, an allowlisted performance
suite, that suite's balanced pair count and verification policy/worker count, passing
source/destination proofs and complete local resource records. Existing performance suites require
four balanced pairs and full-content proofs. `bulk-1g-metadata` requires two balanced pairs and
explicit `metadata-size-tree-v1` proofs: expected paths, counts, types, sizes and required metadata
are checked, but same-size content corruption is not detected. Its reports disclose that content
integrity was not checked. Inspection, approval and provenance share the expected verification
contract; metadata results cannot impersonate a full-content suite. Smoke, failed, cancelled,
incomplete and unapproved PR results cannot pass. The approved derived history copy labels its
revision as the **approved candidate**, retaining the raw checkout separately. The general result
schema requires that reserved label to agree with structured AWS eligibility, candidate and
repository metadata. This is a consistency check, not authentication: the writer still needs the
independently authenticated policy above. AWS ingestion replaces even internally consistent
candidate-supplied approval claims with its own verified outcome. AWS series IDs include a versioned
contract, original series ID, AMI and baked source so they cannot silently join legacy/Depot or
different-image history. Same-attempt identical staging is a no-op; conflicting content cannot
overwrite `runs/<deterministic-id>.json`. Creating that immutable link commits the record.
Temporary-name cleanup is best effort: an unlink failure cannot revoke a successful commit or mask a
prior staging error. Leftover `.aws-stage-*` names are not history JSON records and can be removed
when no staging operation is active. Staging does not push Git, update Pages or grant any
permissions.

Offline tests exercise complete/partial/cancelled results, malformed and oversized archives, forged
metadata, run/ref substitution, policy mismatch, idempotence and conflicting replay. They cannot
prove future GitHub artifact delivery, AWS image observations, or candidate measurement honesty.

## Public resource diagnostics and setup

The keyless report keeps each frozen public record, qualification verdict and series identity
unchanged. A separate rcp-public-resource-diagnostics-v1 companion can supplement its presentation
with resource observations from the exact versioned raw archive referenced by the retired manifest.
The archive digest and retirement digest remain visible. The exporter does not execute archive
contents, copy retained HTML, or run today's qualification validator again.

Per-copy process fields are user CPU seconds, system CPU seconds, peak RSS in **KiB**, voluntary and
involuntary context switches, and minor and major page faults. Paired suites measure one local
command from outside with GNU `time`: the command, its threads and waited descendants. Matrix suites
record what rcp reports about itself in its JSON summary (`--output-format=json`): the `rcp` process
with its threads and, for a remote copy, the source and the destination `rcpd` as two more sets of
figures. A process counts itself only, so a remote copy's `rcp` figures leave out the `ssh` clients
it starts. A revision from before rcp could print that summary is timed all the same, and has no
figures. CPU time can exceed wall time on multiple threads. Peak RSS is neither whole-host
consumption nor a sum of simultaneous processes. Failed or incomplete resource output stays
explicitly unavailable; it is never filled with zero.

Optional system-sampling summaries are separate diagnostics. Their fixed instrumentation descriptor,
cadence and limits identify their measurement contract. Phase elapsed time, sampled cgroup memory,
cgroup CPU and I/O counter deltas, logical-device I/O, and host aggregate CPU/memory observations
are not per-process attribution. Sampled maxima can miss peaks. A cgroup lifetime high-water mark
can include earlier phases and is not a phase-local maximum. An observed phase end does not
establish phase success. Counter resets, unsupported files, incomplete sampling and missing
historical observations remain explicit.

The public setup page describes the configured design once and merges what the runs recorded into
one table: CPU model/count, host RAM when recorded, kernel, architecture, filesystem, array member
count/model/size and configuration. A value appears once when every recording run agrees. Fields
whose recorded values differ are listed with the runs that recorded each value; a field that an
older run did not record is not a difference. Each run page keeps its own observed setup as a
collapsed appendix, and the companion JSON keeps the stable setup fingerprint. Observed setups
exclude run-varying free-space counters, paths, device identifiers, serials, hostnames, environment
variables and process inventories. Historical guest RAM is not inferred from an EC2 instance type.
The configured design is described separately from observed facts.

All sizes in the file-set description are decimal bytes. The existing 1 KB fixture is 10,240,000
files of 1,000 bytes, or 10.24 GB total. This is already exactly 100 times smaller than the 1.024 TB
100 MB and 1 MB fixtures; no second reduction is applied.

For old records, backfill is presentation-only. The reader first authenticates the frozen public
record and retirement-bound manifest, then reads only the exact retained archive version and
verifies its size and SHA256. A successful record's copy samples must match the frozen samples
before resource fields can supplement it. The companion uses fixed numeric/label allowlists and
independent byte/count bounds. Missing, expired, corrupt or over-budget raw evidence makes only the
supplement unavailable; it cannot replace the frozen verdict or silently erase timing history.

The keyless snapshot's existing 512 MiB source budget includes diagnostic reads. It reserves the
manifest bounds and exact raw-body size **before** requesting data, including failed or partial body
reads; no read retry occurs. Output remains subject to the existing 64 MiB artifact budget.
Snapshots containing companions use the version-two artifact index; version-one directories stay
readable. Both the frozen record and companion are bound to the current authenticated job's
cross-job inventory. Public records do not gain service credentials or write permissions, but daily
backfill can add bounded S3 reads and data transfer; these are operating overhead, not free or an
AWS-enforced spending cap.

## Matrix records and retained evidence

A [matrix run](PRODUCTION.md#benchmark-matrix) publishes record version 4,
`rcp-production-public-report-v4`. Earlier versions are unchanged and keep validating as they were
written. Version 4 adds to version 3:

- `cells`: one entry per file set and scenario, in the suite's order, with `outcome` (`complete`,
  `failed`, `timed_out`, `interrupted`, `not_run`), the `stage` it happened in and a short
  `message`. Stage and message are worker-written text reduced to printable ASCII and cut to 32 and
  256 characters. Cells are present only when the worker's result held the suite's contract.
- `scenario` on every measurement. Measurements exist exactly for the completed cells, and the run
  counts exactly when there is at least one.
- `diagnostics`: at most sixteen run-level notes in the collector's own fixed wording, such as why a
  run did not qualify or that its command was stopped.
- `validation` reads "Not verified: a copy counts when rcp exits cleanly and reports the expected
  file count." and `verification.policy` is `exit-status-summary-v1`.

The diagnostics companion of a matrix record is `rcp-public-resource-diagnostics-v2`: version 1 with
each copy's `scenario`, one measured build, and the small commissioning fixtures among the known
file sets. Copies of an unfinished cell are listed with whatever resources they recorded; they are
never expected among the published samples.

A matrix record with a remote cell gets `rcp-public-resource-diagnostics-v3` instead: version 2 with
`roles` on every copy. A remote copy is three processes, so its `metrics` are the `rcp` process's
own (without the `ssh` clients it starts) and `roles.source` and `roles.destination` hold the same
figures for each `rcpd`, or `null` where a daemon's figures could not be read. `roles` is `null` for
a local copy. A copy without figures for its command publishes none for its daemons. The record
decides the version, so a stored companion never changes it, and the versions do not stand in for
each other.

A run of a suite that also times the other tools (`+tools` in its name) publishes record version 5,
`rcp-production-public-report-v5`. It is version 4 with two differences. The measurement of a tool
cell names what the tool ran in `variant_id` (`rcmp-size`, `rlink-default`, `rrm-default`) where a
copy's is `rcp-default`, and `validation` reads "Not verified: a copy or a tool run counts when its
command exits cleanly and reports the expected counts." A tool cell is a cell like any other. Its
`scenario` names the copies it follows and its tool (`local-rcmp`, `local-rlink`, `local-rrm`,
`remote-rcmp`, `remote-rlink`, `remote-rrm`), its samples are the durations of the tool's three
runs, and it has a series of its own. The suite decides the version, so a suite without tool cells
keeps publishing version 4, and the copies of a version 5 record continue the series of the same
cells in version 4 records.

The diagnostics companion of a version 5 record is `rcp-public-resource-diagnostics-v4`: version 3
where every trial carries the `variant_id` of its own cell, and only a remote copy has `roles`. A
tool run is one process whichever copies it follows, so its `metrics` are the tool's own and its
`roles` is `null`, as for a local copy.

A run of a suite with cells on other storage (`nfs` or `memory` in its name) publishes record
version 6, `rcp-production-public-report-v6`. It is version 5 with two more copy scenarios, `nfs`
(between two mounts of a network filesystem) and `memory` (between two filesystems in memory), and
the tool cells that follow each (`nfs-rcmp`, `memory-rrm` and so on). A cell whose storage the
worker could not prepare is `failed` at stage `storage`, with the reason as its message. The cells
of the arrays in a version 6 record continue the series of the same cells in earlier records.

The diagnostics companion of a version 6 record is `rcp-public-resource-diagnostics-v5`: version 4
where a copy through a mount or in memory is one process like a local copy, with `roles` `null`, and
with one more part, `other_storage`. It has an entry for each of `nfs` and `memory` that the run has
cells on:

- `status` `prepared`, with the figures the worker reported: for `nfs` the protocol `version`, the
  `delay_microseconds` added to every packet, the server `threads` and the `round_trip_microseconds`
  measured before the run; for `memory` the `size_bytes` and `inodes` limits of each root.
- `status` `unavailable` when the worker could not prepare it, or `not_recorded` when its report
  says nothing usable about it.

A run may have six file sets where earlier documents had at most four.

On the pages, a matrix run continues the trend of its file set: the change of method appears as a
marked setup change (`Copy check`), not as a new line. The run page lists every cell with its result
and, for anything not measured, what happened. A run that measured only some of its cells is shown
with an amber badge.

Each scenario has its own trends, and the AWS benchmarks page shows all of them at once as copy
duration: a row of small plots for local copies and a row for remote copies over loopback, with one
column per file set. A file set without a qualified run keeps its place in the row. The plot of a
remote copy also gives its latest run's median relative to the same run's local copy of that file
set, when the run made one. The list of runs and the collection status follow the plots.

Every metric and the data tables are on a page for each scenario, with one section per file set.
`local.html` plots copy duration, CPU time and peak memory. `remote.html` plots copy duration, and
the CPU time and peak memory of the source and of the destination `rcpd`: a column for each of the
three, with the destination's plot under the source's. A run page lays out the same five figures of
a remote copy the same way. The `rcp` command's own figures are in each section's data table. The
remote row, that page and its navigation entry appear with the first run that has a remote cell. A
run page with remote cells summarizes each cell under its file set and scenario and lists the
resources of every process of a remote copy.

Each of the other tools has a page as well, with the first run that has a cell of it: `rcmp.html`,
`rlink.html` and `rrm.html` plot the tool's duration, CPU time and peak memory, one section per file
set. A point holds every run of the tool on that file set in one benchmark run: the three after the
local copies and, when the suite has them, the three after the remote copies, as one median and one
range. The two cells stay separate in the record, on the run page and in the data table, which names
the cells a point holds. A tool's page leaves out the file sets on which it takes milliseconds, and
says so: `rcmp` and `rlink` are plotted for the 1 MB and 1 KB file sets, which hold a million and
ten million files, and `rrm` for all four, since removing a terabyte takes time whatever the number
of files. The AWS benchmarks page gains one row for the tools, each on the 1 KB file set. The
copies' rows, pages and trends are unchanged by tool cells.

Each kind of copy on other storage has a page too, with the first run that has a cell there:
`nfs.html` (copies between two mounts of a network filesystem) and `memory.html` (copies between two
filesystems in memory). Each plots the duration, CPU time and peak memory of the copy for its one
file set, as the page of local copies does: such a copy is one rcp process. The file sets are named
with their storage, "1 KB files on NFS" and "1 KB files in memory", because their files are those of
a file set of the arrays. A tool's page gains a section for each of these file sets, once a run has
a cell on it; a point there holds the three runs of the tool after the one kind of copy. The AWS
benchmarks page gains one row for the two, after the rows of the arrays. The test setup page then
also says how that storage is made. The rows, pages and trends of the arrays are unchanged.

The pages show one generation of file sets as the current one. Until a run of
[the weekly suite](PRODUCTION.md#the-weekly-suite) has measured something, that is the generation
described above. From then on it is that suite's: 200 GB of 1 GB and of 1 MB files, and 2,048,000
files of 1 KB on the arrays and in memory. The AWS benchmarks page, the page of each kind of copy
and of each tool and the test setup page then show and describe those file sets. A copy of 200 GB
and a copy of a terabyte are different measurements, so their runs are never on one line. Each page
of a kind of copy or of a tool keeps the trends of the file sets before, with their data tables,
under a collapsed *Earlier file sets* at its end, and a later run of an earlier suite still adds its
points there. The set of 100 MB files has no successor: its trends are among the earlier ones. The
file set on NFS is the same in both generations, so its trends continue. A tool's page then plots
the sets of 1 KB files only, where the weekly suite times it, and says so. On the arrays the weekly
suite times `rcmp` and `rlink` after the local copies only, so a point of theirs holds three runs
where a point of `rrm` holds six, and the line under a file set says after which copies the tool
ran. The file sets of `matrix-weekly-1`, the weekly suite that was defined first and never ran, are
of no generation: should it run, its measurements are on its run page and in no trend. A run page
summarizes its run on the file sets that run measured, whichever they are. The test setup page
tabulates what the arrays did during the newest run on the current file sets. No record or
diagnostics format changes for this.

The test setup page says what the storage allows. Each array is two of the machine's four NVMe
drives, which AWS's instance-store specification for compute optimized instances gives together as
1,400,000 random reads and 680,000 writes of 4 KiB a second; a terabyte file set is several times
the machine's memory, so a copy of it cannot go faster than the destination array takes writes. The
page states that consequence and tabulates, for the newest run that recorded it, what each array and
the processors did during the copies of every file set and scenario: the rate written and read, the
share of the time each array was busy and the share of processor time that was busy. The figures
come from each copy's sampled command phase in the resource diagnostics, over the time from that
phase's first sample to its last; no record or diagnostics format changes.

The raw evidence behind any run page is retained for 90 days and can be fetched and verified with
`just aws-benchmark-evidence`; see
[Find the evidence of a run](PRODUCTION.md#find-the-evidence-of-a-run).
