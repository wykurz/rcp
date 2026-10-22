# Production admission and collection

This is an initially disabled replacement for manual AWS lease preparation. It is separate from
[the existing qualification controller](DURABLE.md): deploying this template does not migrate an
active lease or authorize a benchmark. The first acceptance path permits only the measured
`bulk-1g-metadata` A/A workload. It does not yet provide A/B comparisons or an enabled benchmark
schedule. Public history publication is implemented as a separate, initially disabled workflow.
Ordinary build and test workflows are independent.

## Request boundary

`aws-benchmark-production.yml` is manual-only and inert unless `RCP_AWS_PRODUCTION_ENABLED` is
`true` on `main`. A source build runs without OIDC credentials; the separate submission job treats
its binaries as bounded data. It uses reviewed controller code from the triggering main commit and
never executes the uploaded binaries with cloud credentials. The build creates one artifact reused
for both A/A sides. Admission checks that each baseline/candidate binary pair has matching size and
SHA-256 digest. These are executable-artifact hashes, not fixture or copied-content scans.

The GitHub role can invoke only the admission function. Admission verifies a second original, signed
GitHub OIDC token against official keys: exact audience, repository/owner IDs, main ref, workflow
path and SHA, event, run and attempt. It obtains fresh public GitHub run metadata rather than
accepting a caller's age claim. Manual requests are owner-only by default. Adding maintainers
requires changing the reviewed AWS trust and workflow guard as well as the server policy.

PR pushes, forks, comments and arbitrary OIDC callers do not authorize paid work. Workflow
concurrency is only a convenience. A single DynamoDB transaction creates the run, reserves its full
allowance, increments daily/monthly/total accounting and claims the exclusive active pointer. The
run key includes the immutable repository ID and GitHub run ID, so reruns cannot obtain another
reservation or allocation. The table retains closed tombstones and has no TTL.

The server policy is disabled by default, with zero allowances and no schedule slots. All policy
fields are required in its canonical stored payload. Pricing must have evidence no older than 31
days. Money is integer micro-USD. The reservation is:

```text
ceil(hourly_microUSD * (runtime_seconds + shutdown_seconds) / 3600) + ancillary_microUSD
```

Successful collection never refunds this reservation automatically. The cumulative ledger must be
explicitly seeded with already reserved bootstrap/development costs; absence is an error, not zero
spending. A reservation is a conservative accounting control, not measured AWS billing or an exact
dollar ceiling. IAM cannot enforce a positive DynamoDB numeric increment; the reviewed admission
code is part of this boundary.

## Runtime and recovery

Capacity requests can retry for at most 12 hours only after explicit no-allocation failures. A
successful or ambiguous allocation consumes the opportunity: uncertainty retains the same token, AZ,
parameters and deadline, and does not justify another allocation after an empty EC2 read. There is
no replacement worker after successful allocation.

The 180-minute allocation envelope starts at the allocation attempt, covering boot, fixture
generation, copies, validation, packaging and upload. A healthy independent expiry scan must precede
launch. Immutable deadline tags accompany instance creation, and an exact-instance shutdown schedule
supplies a second termination path once the ID is known. Guest and GitHub roles cannot change the
guards, deadlines or termination protection. Control-plane failures and asynchronous termination can
still exceed the planning allowance; this is not a strict monetary cap.

Disabling policy blocks new launch intents transactionally and makes the watchdog close a readable
active run. An EC2 call following an already committed intent can still be in flight; DynamoDB and
EC2 are not one atomic transaction. The independent tagged expiry guard remains necessary. If state
is unreadable, immediate active-run termination is unavailable, but the independent expiry scan
still runs. Never disable runtime merely to close admission.

Each worker receives a mailbox scoped to its credential-origin instance ARN. Inputs are sealed to
exact S3 object versions; outputs and retained evidence use create-only writes. A stale publisher
cannot overwrite another instance's mailbox. The worker can read its fixed inputs/window and write
its two outputs, but cannot list the bucket, launch resources or alter IAM. These are the trusted
supervisor's credentials. Candidate workloads retain the existing unprivileged, network-isolated
sandbox and cannot reach metadata, S3 or GitHub. They cannot self-certify trusted controller or
teardown evidence.

Metadata validation verifies expected paths, types, counts, sizes and required metadata, outside
copy timing and with the existing cache behavior. It does **not** establish content integrity or
detect same-size corruption. Existing full-content modes and their measurement identities remain
separate. Reports retain this distinction.

A separate collector periodically checks the one active run. It cannot launch or reserve work. After
the fixed outer horizon plus drain, it requires terminal state-machine and GitHub producer proof, no
pending environment approvals, exact instance/root-volume reconciliation and no live namespace
instances. A failed upload that never submitted a state machine has a separate strict no-submission
proof. Elapsed time alone does not resolve uncertain allocation.

Collection retains version-bound raw evidence, a report, HTML and a hashed manifest before a
revision-fenced retirement transaction releases the active pointer. Missing or inaccessible proof
holds ownership. Closed historical replay uses retained artifacts rather than regenerating history
with a newer renderer. A bounded diagnostic report can describe a failed archive while preserving
its exact raw bytes. The local publication helpers discover only retired manifests, bind each to a
consistent closed tombstone, and download exact retained versions through read-only clients.
Discovery traverses the full retained prefix within page/count/time bounds; it is not an atomic
snapshot, and later scans must revisit the prefix. A separate consistent active-pointer observation
describes pending ownership before a manifest exists, without claiming physical teardown or a
cross-service snapshot.

A separate bounded bundle format binds the downloader revision and repository for credential-free
processing. The public exporter renders escaped allowlisted fields and never publishes retained
HTML. Failed qualifications retain diagnostics without performance measurements. Exact source replay
preserves historical output without today's measurement parser. The history repository is trusted
prior exporter storage: the retirement digest binds source evidence, not arbitrary edited
measurement bytes. An embedded checksum would not replace that trust. The manual-only
`aws-benchmark-publish.yml` workflow separates three jobs: read-only AWS download, credential-free
export, and a GitHub history writer. Each transfer uses the exact current-run artifact ID and a
complete inventory digest passed through trusted job outputs. The embedded reader revision is an
additional binding, not independent authentication. Raw handoff artifacts have one-day retention and
the repository's normal Actions artifact access; they are never committed to history.

Discovery is complete within its page/count bounds. Processing uses a 512 MiB aggregate byte budget
and a ten-minute budget checked between runs, within a fifteen-minute job timeout. Completed subsets
can publish while remaining runs are explicitly deferred. Subsequent scans skip already-published
retirement digests without downloading raw archives again. Pending, held and deferred observations
appear in workflow summaries and do not enter performance history. History has a separate
2,000-record, 64 MiB bound; reaching a bound requires deliberate maintenance rather than silently
dropping data.

The writer receives only public JSON. It reads fixed Git objects in a bare repository, preserves
identical prior records and rejects conflicting records. It creates commits through Git plumbing and
pushes to `aws-benchmark-history` without force, with three bounded fetch/reconcile attempts. The
history branch remains separate from Depot's `benchmark-history`. The GitHub `contents: write` job
token is repository-wide: the reviewed writer restricts its target, but GitHub does not scope that
token to this one branch. Protect main and review changes to this workflow.

Pages renders the public JSON with trusted main code, never retained worker HTML. AWS navigation
appears only when valid AWS history exists. Neither publication nor Pages launches a benchmark. The
publication feature flag defaults off. Its read-only AWS role has a separate `PublicationEnabled`
gate, also false by default; enabling it requires specific deployment approval.

## Storage and privileges

`production-stack.yaml` creates separate private, encrypted, versioned storage and a retained
on-demand DynamoDB table. Staging and instance objects expire after seven days; collected run and
report objects expire after 90 days. Noncurrent versions expire after one day and incomplete uploads
after one day. Run tombstones, policy and accounting do not expire. No NAT gateway, network,
long-lived access key or remote shell is created.

| Principal             | Main authority                                                                                                 |
| --------------------- | -------------------------------------------------------------------------------------------------------------- |
| GitHub submit         | Invoke the exact admission function, only from the reviewed owner/main identity                                |
| Admission             | Read policy, atomically reserve/account/claim ownership, seal staging inputs and start the exact state machine |
| Coordinator           | Run the pinned On-Demand instance configuration after a fenced intent; update only run progress                |
| Watchdog              | Independently find/terminate namespace instances, arm exact shutdown schedules and publish instance mailboxes  |
| Worker                | Read fixed objects and create two output objects under its own instance ARN                                    |
| Scheduled termination | Terminate only matching namespace-tagged instances                                                             |
| State machine         | Invoke only the coordinator                                                                                    |
| Collector             | Read terminal/physical proof, retain fixed evidence files and retire ownership; no launch or budget mutation   |
| GitHub publication    | Read the active pointer, repository run records and fixed retained collection files; no writes or launches     |

The separately disabled publication reader has exact-table `GetItem` for `active` and this
repository's run records, exact-prefix `ListBucket` with `MaxKeys=500`, manifest `GetObject`, and
`GetObjectVersion` for the seven fixed collection files. It has no write, launch, scan, collector
invocation or `PassRole` authority. Its default main subject admits matching jobs from other main
workflows when enabled; IAM does not bind the actor, workflow path or immutable repository/owner IDs
for this read role. Listing exposes key names under the repository's run prefix. IAM also cannot
enforce numeric run IDs, retirement state or manifest-selected versions; the reader validates those
properties. These limits must remain explicit in the deployment approval.

All Lambda reserved concurrencies are zero initially; watchdog and collector rules are disabled.
Admission has a separate gate. There is no recurring benchmark trigger in this change. The
five-minute collector timer only reconciles existing ownership and never submits work.

## Deployment and acceptance checklist

1. Approve the account-resolved disabled change set and exact role/trust policies. Inputs are the
   existing account/region, OIDC provider, public subnet/security group, immutable Lambda object
   version and image/source/launch-template pins. No secret keys belong in GitHub or chat. This
   template exceeds the inline `TemplateBody` limit; use a reviewed immutable `TemplateURL` or a
   separately hashed compact JSON rendering of the same template.
2. Resolve the staged worker-profile dependency. Disabled creation may use reviewed placeholder
   launch pins; activation requires a compatible immutable image and numeric launch-template version
   that references the new profile and bucket. Verify the entire effective launch configuration. A
   placeholder is never launch-ready. Generate its UserData with
   `production_startup.reviewed_startup`; production verifies those exact bytes before persisting a
   launch intent. The old direct startup uses the legacy mailbox and is rejected. The guest service
   timer supplements the independent allocation-start deadline.
3. Review and seed the disabled policy plus existing cumulative ledger. Select an explicit
   development allowance, finite nonzero monthly allowance and run-rate limit; these are required
   even for the first manual acceptance. A recurring operating budget and schedule remain separate
   decisions. Never initialize missing historical reservations to zero or silently refund them.
4. With separate approval, run a tiny **no-worker** S3/IAM canary. Verify conditional CopyObject,
   sealed-version reads, create-only writes and the full mixed DynamoDB admission transaction,
   including rejected direct writes and immutable-field mutations. Positive instance-origin IAM
   verification needs genuine EC2-origin credentials and belongs in a separately scoped worker
   acceptance check; a fabricated policy context does not establish it. AWS's CopyObject API and
   conditional-write guide disagree about conditional copy support; the DynamoDB transaction example
   and general condition-key typing also differ. Offline SDK/mocked tests cannot settle live
   enforcement. Fail closed if the tested operation is unsupported; do not remove safeguards.
5. Complete the old controller's physical/producer reconciliation and disable its admission and
   runtime before activating this independent namespace. Two independent active pointers do not
   provide a cross-stack one-worker invariant.
6. Approve production activation separately. Configure the six non-secret repository values
   `RCP_AWS_PRODUCTION_REGION`, `RCP_AWS_PRODUCTION_SUBMIT_ROLE`,
   `RCP_AWS_PRODUCTION_ADMISSION_FUNCTION`, `RCP_AWS_PRODUCTION_BUCKET`,
   `RCP_AWS_PRODUCTION_AUDIENCE` and `RCP_AWS_PRODUCTION_ENABLED`. Enable runtime with admission
   closed, verify independent scans, then permit only the approved acceptance request. The main-only
   trust intentionally does not admit the historical debug branch; pre-merge cloud validation would
   require a separately reviewed exact temporary lane.
7. Verify one complete A/A acceptance through retained collection and ownership retirement,
   including cancelled/failed producer recovery. Enable public reporting separately after approving
   its exact read-only role, enabling the separate `PublicationEnabled` gate, and setting
   `RCP_AWS_PRODUCTION_PUBLISH_ROLE`, `RCP_AWS_PRODUCTION_TABLE` and
   `RCP_AWS_PRODUCTION_PUBLISH_ENABLED`; it shares the existing production region/bucket variables.
   Pages retains its independent `RCP_BENCHMARK_PAGES` flag. Publication has no recurring trigger in
   this change. Broader comparisons and a few-times-weekly schedule require their reviewed
   contracts, operating allowance and activation decisions. Do not call this initial acceptance
   foundation a complete production cutover.

Rollback closes admission and preserves the independent watchdog/collector while active work
reconciles. Restore an older compatible package only after checking its state schema and immutable
pins. Do not delete retained ownership, accounting, snapshots, artifacts or guards as a shortcut.
Disable runtime only after physical teardown and all late-producer/allocation uncertainty are
resolved.

## Offline validation

Use `nix develop` and `just aws-benchmark-test`, or install the hash-pinned production, operator and
report requirements into an isolated CPython 3.12/3.14 Linux x86_64 environment. The full AWS test
suite is included in lint/CI and makes no live AWS calls. It covers signed JWT failures, stale/rerun
producer identity, transactional admission, uncertain outcomes, kill switches, mailbox isolation,
terminal-proof holds, bounded diagnostics and disabled workflow/stack defaults.

`python -m infra.aws_benchmark.production_package` prepares a new local ZIP from an explicit source
closure and offline pinned wheels. It verifies isolated CPython 3.12 Linux imports/native crypto
with networking blocked and enforces the existing 32 MiB compressed cap. DEFLATE byte
reproducibility requires the same Python/zlib versions, which the receipt records. Package hashes
are immutable deployment pins; a local probe is not published-source or live-IAM acceptance.

References:
[transaction IAM](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis-iam.html),
[CopyObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html),
[conditional-write policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html).
