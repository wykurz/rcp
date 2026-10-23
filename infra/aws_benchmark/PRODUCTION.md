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
reservation or allocation. Only attempt 1 can create the initial reservation: GitHub reruns retain
the original actor identity and therefore are not proof that the owner pressed rerun. A failed
initial request needs a new owner-requested workflow run, not an attempt that reuses another actor's
authority. The table retains closed tombstones and has no TTL.

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
proof. A sealed request whose exact named execution never started has a separate `NEVER_STARTED`
proof only after the full horizon and drain, with no allocation intent or worker evidence, exact
execution absence and an empty namespace. Neither proof refunds its reservation. Elapsed time alone
does not resolve uncertain allocation.

GitHub is the submitter, not the worker. A terminal cancelled or failed submission job does not
invalidate independently successful worker measurements; the original conclusion remains visible.
Legacy controller results still require their execution job to succeed. If the watchdog previously
observed worker completion but its status/archive has since expired, collection holds for evidence
recovery instead of relabeling it as a proven failed benchmark.

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

DynamoDB condition checks retain exact table/item fences without relying on an undocumented
`EnclosingOperation` context key. Put/Update/Delete remain transaction-only with their attribute
fences. The service authorization reference and transaction guide disagree about that condition on
`ConditionCheckItem`; offline tests do not replace the no-worker live canary.

All Lambda reserved concurrencies are zero initially; watchdog and collector rules are disabled.
Admission has a separate gate. There is no recurring benchmark trigger in this change. The
five-minute collector timer only reconciles existing ownership and never submits work.

## Authenticated GitHub proof reads

Admission and collection use a dedicated owner-managed GitHub App, proposed name
`rcp-benchmark-reader-wykurz`, installed only on `wykurz/rcp` (repository ID `655073755`, owner ID
`483465`). It needs repository **Actions: read** and **Metadata: read**, no account permissions,
webhooks or user OAuth. The App is a proof reader, never a trigger or environment approver. Signed
caller OIDC, exact main/workflow/requester checks, reservations, ownership and independent shutdown
remain separate authorization boundaries.

Actions read also permits reading workflow logs and artifacts; GitHub has no narrower run-metadata
permission. The application restricts reads to its fixed run, attempt, jobs and pending-deployment
paths. The installation token is read-only for this repository, but the App signing key has broader
App-level authority: a signed App JWT can suspend or delete its own installation. The URL allowlist
does not constrain a stolen key. Keep this a dedicated, owner-only App with only this installation.

The template references an **external** Secrets Manager secret named
`/rcp/production/github-read-app` in account `381183037896`, region `us-east-1`, encrypted with the
AWS-managed `aws/secretsmanager` key. Its secret value is only the App's PEM private key. The
template creates neither a secret nor a key. Only `AdmissionRole` and `CollectorRole` gain
`secretsmanager:GetSecretValue` on the complete resolved ARN, including its six-character suffix,
with `StringEquals: secretsmanager:VersionStage: AWSCURRENT`. Every read explicitly requests that
stage. No list, describe, write, rotation, role-passing, extra KMS or trust permissions are added.
Worker, coordinator, watchdog, publication and GitHub job roles receive no secret access; existing
account administrators retain their existing powers.

| Template parameter        | Non-secret Lambda configuration             |
| ------------------------- | ------------------------------------------- |
| `GitHubAppClientId`       | `RCP_PRODUCTION_GITHUB_APP_CLIENT_ID`       |
| `GitHubAppInstallationId` | `RCP_PRODUCTION_GITHUB_APP_INSTALLATION_ID` |
| `GitHubAppSecretArn`      | `RCP_PRODUCTION_GITHUB_APP_SECRET_ARN`      |

These parameters default empty. The secret-read policies are absent until all three are supplied;
CloudFormation rejects runtime or admission enablement with missing configuration. Parameter
constraints reject malformed IDs, partial/wildcard ARNs and secrets outside the approved
account/region/name. Porting this setup requires reviewing that explicit scope. Existing watchdog
conditions are unchanged: a rejected configuration update must not silently disable active guards.
Complete configuration can be deployed with all feature flags off for a separately approved
no-worker canary; disabling runtime alone does not revoke configured secret-read policies.

At runtime, fetch AWSCURRENT only to mint a token. Sign a short RS256 App JWT, then POST only to the
fixed installation token endpoint with the exact repository ID and read permissions. A fixed,
bounded `GET /installation/repositories?per_page=2` verifies the minted token sees exactly the
configured repository; it is a scope check, separate from the four run-proof path families. Cache
only the installation token in Lambda memory, until five minutes before its returned expiry and at
most 55 minutes after minting. Do not cache proof responses or persist PEM/JWT/tokens in environment
variables, disk, logs, artifacts or state. Dropping Python references does not guarantee secure
memory erasure. Secret retrieval, minting, proof reads and at most one 401 refresh share finite call
and time budgets: one observation allows 20 seconds and at most 13 remote calls, including Secrets
Manager, minting and repository-scope validation. Socket timeouts do not hard-bound DNS resolution
or header parsing; the outer Lambda limits remain 45 seconds for admission and 180 seconds for
collection, and a late operation fails the shared-budget check. Missing/revoked credentials,
malformed scope or timeout hold admission/retirement; there is no anonymous fallback. Quota backoff
defers work rather than extending an invocation. GitHub failure must not disable the independent AWS
watchdog or release uncertain ownership.

The following setup is proposed, **not provisioned by this PR**:

1. Obtain separate approval to create the owner-only App and install it on this selected repository,
   acknowledging both the logs/artifacts read scope and App-key installation authority. Keep other
   repository/account permissions off, webhook delivery inactive and user authorization unused.
   Record the exact client and installation IDs from the owner's GitHub UI; these may be shared.
2. Approve and review the one-time secure private-key import procedure, generate a key and store
   only its PEM in the exact external secret with AWS-managed encryption. Never paste it into chat
   or command arguments, put it in CloudFormation parameters, or commit it. The application requires
   no App OAuth client secret. Record the full secret ARN and public key fingerprint, never the key.
3. Prepare an account-resolved disabled change set using the three identifiers and immutable package
   pins. Review the two exact secret-read grants and unchanged trust before deployment. App setup,
   secret creation/import and live IAM deployment each remain outside code-preparation authority.
4. With separate approval, perform the **no-worker** live canary: fresh token mint, exact scoped
   metadata reads, expiry/refresh and sanitized denial behavior; verify AWSCURRENT succeeds while
   other stages/secrets and other runtime principals are denied. Verify the watchdog remains
   independent. Record rate-limit headroom without printing tokens. Offline tests are not this IAM
   or operational evidence; **F13 remains open until the live acceptance gate passes**.

The owner must maintain a key rotation and emergency procedure because App private keys never expire
by themselves. Rotate with overlapping keys: securely update AWSCURRENT, verify fresh mint/read
success, then retire the old key after the bounded token overlap. Moving AWSCURRENT or deleting a
key must not be assumed to revoke already-issued installation tokens. For compromise, close AWS
admission first, preserve watchdog/ownership, suspend the installation through the owner UI, and
rotate/revoke the affected key. Access failure can delay retirement; it cannot justify another
worker.

References:
[App token scope and lifetime](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-an-installation-access-token-for-a-github-app),
[App installation authority](https://docs.github.com/en/rest/apps/apps#delete-an-installation-for-the-authenticated-app),
[key rotation](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/managing-private-keys-for-github-apps),
[installation suspension](https://docs.github.com/en/apps/maintaining-github-apps/suspending-a-github-app-installation),
[secret access](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html),
[secret condition keys](https://docs.aws.amazon.com/service-authorization/latest/reference/list_secretsmanager.html).

## Deployment and acceptance checklist

1. Approve the account-resolved disabled change set and exact role/trust policies. Inputs are the
   existing account/region, OIDC provider, public subnet/security group, immutable Lambda object
   version and image/source/launch-template pins, plus the non-secret App identifiers described
   above. No secret keys belong in GitHub or chat. This template exceeds the inline `TemplateBody`
   limit; use a reviewed immutable `TemplateURL` or a separately hashed compact JSON rendering of
   the same template.
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

## Maintenance and unresolved production prerequisites

Close the separate admission gate and wait for ownership retirement before rotating policy version,
price/cap configuration, image/template pins, workload plan, or Lambda package/validator semantics.
Keep the admitted policy enabled during an ordinary drain; setting its enabled field false is the
intentional emergency kill switch. The exact version and pin fences are safety checks, not automatic
migration. Keep the collector's validator compatible until retirement too: if a retained-open
verdict conflicts after a package change, restore the exact compatible validator and reconcile its
immutable evidence. Do not overwrite the manifest or release ownership to escape that hold.

Authenticated GitHub proof reads still require separately approved App/secret setup, deployment and
live no-worker acceptance. This PR prepares the narrow undeployed secret-read policies; it creates
no credentials or live access. Collection reuses one terminal producer observation within an
invocation while repeating physical/execution checks before retirement. No stale producer proof is
accepted across invocations. Admission failures and collector holds emit bounded structured
diagnostics without tokens, upload URLs or SDK response bodies. An operator alarm/notification
destination remains an activation decision.

The dormant scheduled policy requires its fixed baseline to equal main for this initial A/A
contract; it is not a working recurring comparison policy as main advances. Keep schedule slots
empty. A future scheduled contract, baseline selection and operating budget need their own tests and
approval before adding a timer.

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
