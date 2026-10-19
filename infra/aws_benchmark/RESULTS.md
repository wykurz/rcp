# AWS result ingestion and reporting

This debug integration retains the exact temporary `aws-benchmark-debug-20261019` branch exception
for validation, controller receipts and diagnostic reporting. The workflow records the actual debug
ref and SHA; it never labels them as main. Offline history admission still requires exact main, even
with a matching policy. Keep this exception out of the main PR and keep live publication disabled.

The manually dispatched AWS workflow now retains a report as well as raw evidence. This change does
not deploy infrastructure, rebuild an AMI, change AWS permissions, run a benchmark, enable a history
publisher, or replace Depot. Depot continues to supply its existing smoke, performance, history and
Pages jobs while AWS coverage is evaluated separately.

## Read a run

The trusted controller uploads `aws-benchmark-results`: `status.json`, `results.zip` when available,
and a separate `controller.json` receipt. A fresh GitHub-hosted job downloads the exact artifact ID
returned by that upload step, validates it, adds a job summary and uploads `aws-benchmark-report`.
Both result artifacts have 90-day retention, subject to the repository's retention limit. Build
artifacts remain at one day; the S3 mailbox remains at seven days. No new persistent AWS storage is
created. Cancellation can prevent the controller upload or report job from running; these are
best-effort diagnostics, not the shutdown safety boundary.

Download the report and open `index.html` locally. Its `dashboard/index.html` is the existing
standalone measurement dashboard. The directory also contains:

- `raw/`: byte-for-byte copies of the bounded input files, without extracting `results.zip`.
- `provenance.json`: validation status, producer conclusion, controller/worker observations and
  archive/member SHA-256 fingerprints.
- `summary.md`: generated provenance and median/repeat tables. Archived `summary.md` is never used
  as trusted Markdown or HTML.
- `derived.json`: a separate validated dashboard record, when measurement data is available. Its
  deterministic AWS run identity replaces the worker UUID only in this derived copy; the original
  UUID and revision remain in provenance. Diagnostic records have purpose `diagnostic`, excluding
  them from performance trends and historical change comparisons.

Missing, malformed, oversize or digest-mismatched input produces a diagnostic report and a failed
ingestion step. Available bounded raw files are retained. Oversize or special files are not copied.
Worker failure can still produce a valid partial dashboard; it never implies a successful
comparison. Producer cancellation overrides any apparently successful worker record for history
eligibility. Unknown observations remain unknown.

## Provenance and trust

These revisions have different meanings:

| Field                            | Source and meaning                                                                                                                                                               |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Candidate and baseline           | Pinned dispatch inputs, bound to the controller request and its binary hashes. They identify the requested builds.                                                               |
| Controller and workflow SHA      | GitHub's exact dispatch checkout/workflow identity. Admission requires literal main or the exact temporary debug ref, the matching repository/workflow, and `workflow_dispatch`. |
| Controller observed AMI          | `ImageId` and instance type observed through the already permitted `ec2:DescribeInstances`. At most three bounded observation attempts; missing data is null.                    |
| Supervisor reported baked source | `worker.json.harness_revision`, read by the root supervisor from the baked `revision.txt`. This is not an independently queried EC2 image tag or the candidate SHA.              |
| Raw harness checkout             | The unchanged measurement record's `revision`. The baked tree normally has no `.git`, so this is null. It must not be relabeled as the candidate.                                |

The receipt also records the operator window, binary request hashes, downloaded status/archive
hashes, controller outcome and cleanup verification. It is written before launch and updated on
completion; receipt write failures after launch cannot bypass cleanup. An incomplete receipt never
proves teardown. The independent scheduled cutoff is unchanged, can execute up to two minutes late,
and termination remains asynchronous; the report makes no exact billing-cap claim.

The parser accepts only the worker's single-disk, stored, comment-free ZIP format. It checks the
central directory count before allocating ZIP entries: at most 1001 members and 64 MiB total archive
and expanded data. It rejects ZIP64/extra fields, encryption, overlap, hidden/trailing records,
duplicates, non-ASCII or unsafe paths, symlinks/special files and CRC failures. It never extracts or
executes a member. Worker/status/receipt JSON is at most 1 MiB; measurement JSON is at most 8 MiB.
Strict JSON rejects duplicate keys/nonfinite numbers and limits depth to 40, nodes to 100000 and
strings to 16384 characters. Measurement collections are bounded to 16 cases, 16 variants, 512
trials and 256 summaries before the existing schema/proof validation runs. These are protocol
limits, comfortably above the currently allowlisted one-case suites.

Candidate binaries share the unprivileged harness UID and can forge candidate-owned measurement
files. Hashes and validation establish integrity and traceability, **not honest-measurement
attestation**. Root supervisor metadata and controller observations are separate evidence. The
controller never parses the ZIP and the report job has only `contents: read`: no protected
environment, OIDC, AWS credentials, history write token, candidate checkout or candidate execution.
Candidate artifact-name collisions fail closed; there is no fallback to names, newest artifacts,
another workflow run or caller-supplied artifact IDs.

The cost boundary remains manual dispatch, a reviewed exact allowed ref, the protected environment,
explicit operator arming and independently scheduled closure. Reporting introduces no paid-launch
trigger, `workflow_run`, `pull_request_target` or `repository_dispatch` path. An attacker can
consume normal GitHub queue capacity if repository permissions permit dispatch, but cannot open an
ASG window. The environment OIDC subject is not workflow-specific: administrators must protect main
and review all workflows able to use that environment. Any temporary external debug-branch exception
is a separate, coordinated permission decision; this change neither edits nor relies on that
exception.

## Offline ingestion and future history staging

No extra AWS information, credentials, account changes or AMI rebuild are needed for this reporting
change. The existing setup checklist remains authoritative for a future live benchmark. To inspect
downloaded evidence offline, supply the independently verified identity from the validation job's
`RCP_AWS_RUN_IDENTITY` line, and the producer job's actual conclusion:

```bash
just aws-benchmark-report downloaded-raw --identity trusted-identity.json \
  --conclusion cancelled --output new-report-directory
```

Each output directory must be new, preventing stale success pages after failed retries. The input
and output prefixes must be operator-owned and kept out of concurrent untrusted write control. This
command needs Python and repository code only; it makes no network calls.

Future history admission is implemented **only as an offline gate and local staging operation**.
There is no activated publisher, write permission or dispatch to Pages. `benchmarks.publish` is a
storage helper, not a provenance gate; never feed it arbitrary AWS worker files or a diagnostic
record. A later publisher requires a separate reviewed authorization design.

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

`identity` is the full object from the trusted validation log, including repository, workflow/ref,
run ID-attempt, run URL, event, controller/workflow SHA, suite and candidate/baseline SHAs. The
approver must separately establish that both build revisions are approved for performance history
and that the AMI/baked source pair is trusted. Do not generate this policy from downloaded evidence,
accept it from a candidate PR, or treat a policy boolean as authentication. The policy pins one
exact attempt and archive, preventing substitution of an otherwise valid earlier bundle.

```bash
just aws-benchmark-report downloaded-raw --identity trusted-identity.json \
  --conclusion success --policy independent-policy.json \
  --history local-staging --output new-approved-report-directory
```

Staging additionally requires successful producer/controller/supervisor outcomes, verified cleanup,
observed `c5d.24xlarge`, matching image/baked source/binary digests, an allowlisted performance
suite, four balanced adjacent pairs, full source/destination proofs and complete local resource
records. Smoke, failed, cancelled, incomplete and unapproved PR results cannot pass. The approved
derived history copy labels its revision as the **approved candidate**, retaining the raw checkout
separately. AWS series IDs include a versioned contract, original series ID, AMI and baked source so
they cannot silently join legacy/Depot or different-image history. Same-attempt identical staging is
a no-op; conflicting content cannot overwrite `runs/<deterministic-id>.json`. Staging does not push
Git, update Pages or grant any permissions.

Offline tests exercise complete/partial/cancelled results, malformed and oversized archives, forged
metadata, run/ref substitution, policy mismatch, idempotence and conflicting replay. They cannot
prove future GitHub artifact delivery, AWS image observations, or candidate measurement honesty.
