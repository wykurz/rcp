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
