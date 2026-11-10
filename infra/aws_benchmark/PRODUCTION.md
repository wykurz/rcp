# Production admission and collection

This is an initially disabled replacement for manual AWS lease preparation. It is separate from
[the existing qualification controller](DURABLE.md): deploying this template does not migrate an
active lease or authorize a benchmark. The first acceptance path permits only the measured
`bulk-1g-metadata` A/A workload. It does not yet provide A/B comparisons or an enabled benchmark
schedule. Public history publication is implemented as a separate, initially disabled workflow.
Ordinary build and test workflows are independent.

## Request boundary

`aws-benchmark-production.yml` is inert unless `RCP_AWS_PRODUCTION_ENABLED` is `true` on `main`.
Owner-requested manual runs and the separately gated proposed schedule share the same submission
path; a missing `RCP_AWS_PRODUCTION_SCHEDULE_ENABLED` never enables scheduled builds. A source build
runs without OIDC credentials; the separate submission job treats its binaries as bounded data. It
uses reviewed controller code from the triggering main commit and never executes the uploaded
binaries with cloud credentials. The build creates one artifact reused for both A/A sides. Admission
checks that each baseline/candidate binary pair has matching size and SHA-256 digest. These are
executable-artifact hashes, not fixture or copied-content scans.

The GitHub role can invoke only the admission function. Admission verifies a second original, signed
GitHub OIDC token against official keys: exact audience, repository/owner IDs, main ref, workflow
path and SHA, event, run and attempt. It obtains fresh authenticated GitHub App run metadata rather
than accepting a caller's age claim. Manual requests are owner-only by default. Adding maintainers
requires changing the reviewed AWS trust and workflow guard as well as the server policy.

The admission verifier deliberately accepts only tokens whose signed `exp - iat` is at most 600
seconds and whose age is less than 600 seconds; it separately checks expiry and the allowed clock
skew. This is a fail-closed operational assumption, not a promise that GitHub will always issue that
lifetime. Verify the actual signed claim shape during live acceptance without logging tokens. A
future issuer lifetime change can reject all admission requests; review that change and its tests
before changing the bound. App installation-token lifetimes are a separate contract below.

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

### Bind admission to a deployment during cutover

The policy envelope can include a fifth attribute,
`launch_pins_sha256: {"S": "<64 lowercase hex characters>"}`. Compute it with
`production_policy.launch_pins_sha256(config.pins())` from the actual numeric launch-template
version, AMI, worker source revision and remaining validated deployment pins. This is a
domain-separated canonical digest, not a credential or a hash of the controller ZIP. The production
handler supplies the pins captured when it built the admission service. A bound envelope with
missing or mismatched pins rejects before a new reservation, GitHub App read or upload capability.

Legacy four-attribute envelopes retain their existing behavior. Older deployed handlers reject the
new fifth attribute. For a production cutover, use both the binding and a **new immutable policy
version**: the new version rejects a decision prepared before the fence, while the envelope binding
rejects an old invocation that reads policy only after the deployment. Reusing the old enabled
version is unsafe. A fixed wait derived from Lambda's Invoke timeout alone does not prove that all
old configuration has drained; initialization and invocation are separate lifecycle phases.

Keep the existing safeguards and use a reviewed operator sequence:

1. In one serializable transaction, require the active owner to be absent and conditionally disable
   the exact current policy envelope. Preserve its raw payload field set, counters and calendar; do
   not serialize optional Policy defaults into an older stored payload.
2. Keep that fence closed while updating the reviewed code and launch configuration. Verify actual
   code hashes, full configuration, IAM, physical idle state and the independent guard. Check the
   immutable object and actual numeric launch-template version, not guessed identifiers.
3. Reopen only after successful verification, using a bound envelope for the target pins and a new
   immutable enabled version. Preserve the prior allowance, requester and calendar authority.
4. On failure or uncertainty, keep admission closed until a compatible target and its exact pins are
   verified. A rollback to legacy code cannot simply remove the binding and reopen: a newer
   invocation may still hold the other configuration. Use compatible forward recovery. An optional
   envelope-aware rollback target must be proven against the complete old launch configuration,
   including worker/startup compatibility; parser support alone is insufficient.

This code adds no policy, permission or deployment by itself. Live cutover, restoration and recovery
remain separately reviewed operations. See the AWS
[update-state contract](https://docs.aws.amazon.com/lambda/latest/dg/functions-states.html) and
[execution lifecycle](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html).

## Runtime and recovery

Capacity requests can retry for at most 12 hours only after explicit no-allocation failures. A
successful or ambiguous allocation consumes the opportunity: uncertainty retains the same token, AZ,
parameters and deadline, and does not justify another allocation after an empty EC2 read. There is
no replacement worker after successful allocation.

Launch evidence distinguishes a request that was never sent, a definitive rejection, an accepted
allocation and an unknown outcome. A persisted send sequence fences a late negative response from
superseding a later send or positive allocation evidence. Retrying the exact unresolved token is
separate from admitting useful benchmark work: a replay keeps the original capacity/allocation
cutoffs and guard checks, while publication still requires the workload's remaining-time budget. A
fresh request rejected definitively for permissions, quota or parameters closes without claiming an
allocation; it does not start another paid attempt automatically.

An earlier ambiguous send followed by a capacity error remains unresolved. EC2 documents successful
idempotent replay after an original request completes, but does not provide a 60-second upper bound
on an original in-flight operation. Waiting that long is not proof of no allocation. Keep the owner
and independent guard until positive allocation/termination evidence resolves the uncertainty; do
not release ownership or choose a new token from an empty Describe response or elapsed time. See
[EC2 idempotency](https://docs.aws.amazon.com/ec2/latest/devguide/ec2-api-idempotency.html).

The 180-minute allocation envelope starts at the allocation attempt, covering boot, fixture
generation, copies, validation, packaging and upload. A healthy independent expiry scan must precede
launch. Scan freshness remains exactly `0 <= age <= GUARD_GAP_SECONDS` (120 seconds). A future scan
is never accepted early: at most three consistent reads, with two 0.25-second waits, can observe
local time catching up. The one-second monotonic retry budget bounds retry scheduling and waits;
existing SDK timeouts bound each read separately. The clock is sampled after each read and before
launch. Persistent future, missing or stale scans defer without allocation; malformed, foreign or
unhealthy proofs fail closed. Sanitized nonfresh observations distinguish clock holds without
retaining raw guard payloads. No retry refreshes an allocation or producer deadline. Immutable
deadline tags accompany instance creation, and an exact-instance shutdown schedule supplies a second
termination path once the ID is known. Guest and GitHub roles cannot change the guards, deadlines or
termination protection. Control-plane failures and asynchronous termination can still exceed the
planning allowance; this is not a strict monetary cap.

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
Likewise, a failed, aborted or timed-out coordinator loop does not invalidate successful worker
measurements after exact allocation binding, physical cleanup and terminal producer proofs pass. The
coordinator's terminal status remains in the retained report and public note. Legacy controller
results still require their execution job to succeed. The watchdog commits the bounded terminal
status and its exact version/hash evidence, including during closing ticks. That status carries the
archive digest when an archive was produced. The watchdog does not HEAD the archive or read old
object versions; the collector binds archive bytes using its existing output-read permissions.
Committed status survives mailbox lifecycle expiry in the service record. Generic access denial
never proves deletion, and this path adds no ListBucketVersions grant. Production state/receipts
share a 192 KiB bound, including the 720-attempt failure inventory and the exact bounded status
bytes stored as canonical base64. This avoids JSON control-character expansion; decoding still
verifies the original length and digest. The independent 400 KiB DynamoDB item check and legacy
receipt limits are unchanged. Missing committed or indicated output holds for recovery until nine
days after the bound allocation deadline. That grace accounts for seven-day expiry, UTC rounding and
one-day noncurrent retention; time alone does not prove deletion. Only confirmed object absence
after that grace can retire the run as **evidence unavailable**, with unknown workload outcome,
never as a proven failed benchmark. Uncommitted missing output also holds from seven days until that
boundary. Access denial, transient reads and checksum failures still hold rather than becoming
absence evidence. The frozen `mailbox-loss-v1` recovery policy prevents later defaults from
reinterpreting unavailable outcomes.

Collection retains version-bound raw evidence, a report, HTML and a hashed manifest before a
revision-fenced retirement transaction releases the active pointer. Missing or inaccessible proof
holds ownership except for the explicit evidence-unavailable outcome above. The collector freezes
the public projection in the retained report before retirement; later first publication of those
records does not use today's measurement parser. Both the frozen projection and explicit
qualification outcome are required; no pre-deployment legacy fallback reinterprets missing fields.
Collection first commits a bounded `report.json` checkpoint containing the run/state binding, exact
source references and presence, collection time, verdict, and controller/summary digests. Only then
are the other immutable objects and final versioned manifest written. An interrupted retry recovers
matching retained input copies before consulting the original sources and preserves committed
presentation bytes. It still revalidates the current qualification verdict and repeats physical and
execution checks before the fenced retirement transaction. Competing collectors cannot replace the
winning checkpoint or retire twice. Missing input bytes that were never copied cannot be
reconstructed from a digest after source expiry: collection holds rather than changing the frozen
verdict. Partial sets from an older collector without a checkpoint also require explicit recovery;
they are never silently omitted or adopted. Completed older manifests remain readable. A missing
derived object whose renderer has changed requires compatible code to reproduce its committed bytes.

### Keyless retained reporting

Public v1 success/failure records, v2's distinct evidence-unavailable outcome and v3's canonical
collector-recorded `collected_at` remain readable. Older records show no trusted collection time;
they never promote the candidate-controlled harness timestamp. Measurements remain self-reported and
unattested: candidate binaries share the harness UID and can forge their output. Verified cleanup
describes independent teardown evidence only. Metadata validation does not establish content
integrity.

The initial dashboard uses objects still retained under the existing **90-day AWS lifecycle**.
Lifecycle expiration makes objects eligible for deletion; it is not an exact deletion time or a
strict timestamp filter. This is not permanent history. A later complete scan removes expired
records from the site; if reporting is stopped or held, the previously deployed static site remains
available and can be older. Every page shows the snapshot observation time. Extending retention or
publishing a permanent archive requires a separate decision.

`aws-benchmark-publish.yml` reads each frozen public projection directly. A consistent closed
DynamoDB tombstone anchors the retirement manifest digest; an immutable-version reread binds the
manifest, exact report/controller versions bind the projection and context, and a final consistent
tombstone reread checks stability. This authenticates the collector's frozen bytes, not the honesty
of the measured candidate. It does **not** download raw archives, reparse measurements, render
retained worker HTML, recollect, retire ownership or write Git.

The read-only download job has Contents read and OIDC request permission, with no environment so the
existing main-ref AWS subject is preserved. It installs hash-pinned dependencies before requesting
one 15-minute AWS reader session. It reads only through the existing publication role. No writer
App, signing key, environment approval or `aws-benchmark-history` branch is needed by this path.
Existing writer protections and the separately disabled legacy canary remain untouched; their
procedures in [TRUST.md](TRUST.md) are not activation prerequisites for keyless reporting.

A small public-JSON artifact crosses jobs by exact current-run artifact ID and a complete inventory
digest from trusted predecessor outputs. The embedded reader revision is only a consistency check.
The directory renderer repeats inventory, schema and identity checks with no AWS credential, secret
or Git write token. It renders escaped allowlisted fields alongside project docs and separate Depot
history. The handoff artifact expires after one day; AWS source retention remains 90 days.

Discovery must complete within its page/count bounds. Processing has a 512 MiB source allowance, 64
MiB public-output bound and ten-minute budget checked between runs, within a fifteen-minute job
timeout. Each public-record attempt reserves the maximum of two manifest reads, a controller and a
report. Pending ownership is displayed without claiming teardown. Any held, deferred or incomplete
scan prevents deployment and preserves the previous site; a valid empty scan publishes an explicit
empty AWS view. There is no partial-success fallback that silently erases failed records.

### Publication activation and deployment boundary

This code does not enable access or publication. Keep `RCP_AWS_PRODUCTION_PUBLISH_ENABLED=false` and
the stack's separate `PublicationEnabled=false` until an exact live read/deploy acceptance is
approved. The latter still controls the existing role's trust and read grants. There is no new IAM
scope, bucket, secret or App in this change. Runtime, admission, policy counters and paid execution
remain independent. `RCP_AWS_HISTORY_TRUST_READY` is exclusively a dormant legacy writer/canary
control and stays false; it is not a prerequisite or substitute proof for the keyless reader.

The owner-only main workflow can render on push, manual dispatch, or a separately gated daily poll.
It and Pages reject reruns and stale main. A credential-free job renders and packages the full site;
a separate `github-pages` job has Pages write/OIDC only for deployment, never AWS reads or artifact
execution. It selects the unique immutable Pages artifact name within that same workflow run. Source
freshness is rechecked after any deployment wait. This is a fresh observation before a network
action, not an atomic main lock.

`RCP_BENCHMARK_PAGES` stays an independent gate. While keyless publication is enabled, direct
docs-only Pages runs skip deployment so they cannot replace the dashboard with a site lacking AWS
records. The keyless workflow builds the whole site instead. Non-owner pushes cannot assume the
owner-bound AWS role, so they leave the old site until the next authorized owner manual/scheduled
refresh. Quiesce pending Pages/reporting runs before changing mode flags: job-context variable reads
are defense in depth, not an atomic live settings lock. Do not enable the legacy writer lane
alongside this mode. Neither reporting path launches a benchmark.

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
invocation or `PassRole` authority. Its trust retains the configured GitHub OIDC provider, audience
`sts.amazonaws.com` and subject `repo:wykurz/rcp:ref:refs/heads/main`. It additionally requires
exact `StringEquals` claims for repository `wykurz/rcp`, repository ID `655073755`, repository owner
ID `483465`, actor ID `483465`, ref `refs/heads/main` and workflow
`Publish retired AWS benchmark history`. The native claims are documented in the
[AWS GitHub condition-key reference](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_iam-condition-keys.html#condition-keys-wif).
The workflow claim is a display name, not an immutable path or revision; another owner-run main
workflow using that name can still match. `job_workflow_ref` applies to reusable workflows and is
not used here. Preserve main review/protection and separately verify real role-assumption positives
and negatives before enabling this role. Listing exposes key names under the repository's run
prefix. IAM also cannot enforce numeric run IDs, retirement state or manifest-selected versions; the
reader validates those properties. These limits must remain explicit in the deployment approval.

DynamoDB condition checks retain exact table/item fences without relying on an undocumented
`EnclosingOperation` context key. Put/Update/Delete remain transaction-only with their attribute
fences. The service authorization reference and transaction guide disagree about that condition on
`ConditionCheckItem`; offline tests do not replace the no-worker live canary.

All Lambda reserved concurrencies are zero initially; watchdog and collector rules are disabled.
Admission has a separate gate. Recurring trigger declarations remain inert without their separate
exact-true repository gates and approved server policy. The five-minute collector timer only
reconciles existing ownership and never submits work.

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
`/rcp/production/github-read-app` in the deployment account and region, encrypted with the
AWS-managed `aws/secretsmanager` key. Its secret value is only the App's PEM private key. The
template creates neither a secret nor a key. Only `AdmissionRole` and `CollectorRole` gain
`secretsmanager:GetSecretValue` on the complete resolved ARN, including its six-character suffix,
with `StringEquals: secretsmanager:VersionStage: AWSCURRENT`. Every read explicitly requests that
stage. These App-specific grants add no list, describe, write, rotation, role-passing, KMS or trust
permissions. They are separate conditional `AWS::IAM::RolePolicy` resources with explicit
`DeletionPolicy: Delete` and `UpdateReplacePolicy: Delete`. Successful stack removal or replacement
removes these grants even when `CollectorRole` is retained for recovery; verify actual removal if
CloudFormation cleanup fails. This does not revoke an already-minted token or erase an exposed key.
Never delete the stack to close admission during active reconciliation. Worker, coordinator,
watchdog, publication and GitHub job roles receive no secret access. The external resource policy
also denies direct reads by root and unselected administrators. Existing policy-management powers
are separate; see
[the explicit reader boundary and recovery procedure](TRUST.md#explicit-secrets-manager-reader-boundary).

| Template parameter        | Non-secret Lambda configuration             |
| ------------------------- | ------------------------------------------- |
| `GitHubAppClientId`       | `RCP_PRODUCTION_GITHUB_APP_CLIENT_ID`       |
| `GitHubAppInstallationId` | `RCP_PRODUCTION_GITHUB_APP_INSTALLATION_ID` |
| `GitHubAppSecretSuffix`   | `RCP_PRODUCTION_GITHUB_APP_SECRET_ARN`      |

Supply the secret's exact six-character generated suffix; the template constructs its complete ARN
from the deployment partition, region and account plus the fixed secret name. The Lambda interface
still receives that full ARN, which the runtime binds to its configured account, region and
supported `aws` partition. This removes an unnecessary account-specific template literal; account
IDs are not credentials. It does not extend runtime support to another partition or change the
allowed secret name. Record the resolved full ARN in the separate account-specific deployment
approval.

These parameters default empty. The secret-read policies are absent until all three are supplied;
CloudFormation rejects runtime or admission enablement with missing configuration. Parameter
constraints reject malformed IDs and suffixes; no wildcard or partial ARN is used for secret access.
Existing watchdog conditions are unchanged: a rejected configuration update must not silently
disable active guards. Complete configuration can be deployed with all feature flags off for a
separately approved no-worker canary; disabling runtime alone does not revoke configured secret-read
policies.

At runtime, fetch AWSCURRENT only to mint a token and sign a short RS256 App JWT. Before each mint,
fixed JWT-authenticated `GET /app` and `GET /app/installations/{configured_id}` reads verify the App
client ID, App/installation association, owner/account IDs and names, exact Actions/Metadata read
permissions, empty event subscriptions, one installation, selected-repository mode and no
suspension. All checked fields must exist with the expected types. GitHub documents
[`installations_count`](https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app) in the
App response prose but omits it from the example; a missing field holds safely, and the live canary
must verify the real contract. The
[installation response](https://docs.github.com/en/rest/apps/apps#get-an-installation-for-the-authenticated-app)
does not expose its selected repository inventory. Verify the full installation contains only the
approved repository manually, along with owner-only App visibility and inactive webhook delivery.
Runtime metadata checks do not establish those three properties or constrain a stolen App key.

POST only to the fixed installation token endpoint with the exact repository ID and read
permissions; never mint a broader token to inspect installation scope. A fixed, bounded
`GET /installation/repositories?per_page=2` verifies the **narrowed token** sees exactly the
configured repository, not that the underlying installation has no additional repositories. Cache
only the installation token in Lambda memory, until five minutes before its returned expiry and at
most 55 minutes after minting. App/installation drift is checked at the next mint, not on each warm
token use; cached tokens remain narrowed to the original read scope. Do not cache proof responses or
persist PEM/JWT/tokens in environment variables, disk, logs, artifacts or state. Dropping Python
references does not guarantee secure memory erasure.

Secret retrieval, metadata checks, minting, proof reads and at most one 401 refresh share finite
call and time budgets: one observation allows 20 seconds and at most 17 remote calls, including
Secrets Manager and repository-scope validation. The worst case is five initial authentication
calls, six proof reads, five refresh calls and one repeated proof read. Additional metadata latency
can exhaust that unchanged deadline and hold safely. Socket timeouts do not hard-bound DNS
resolution or header parsing; the outer Lambda limits remain 45 seconds for admission and 180
seconds for collection, and a late operation fails the shared-budget check. Missing/revoked
credentials, malformed scope or timeout hold admission/retirement; there is no anonymous fallback.
Quota backoff defers work rather than extending an invocation. GitHub failure must not disable the
independent AWS watchdog or release uncertain ownership.

The following setup is proposed, **not provisioned by this PR**:

1. Obtain separate approval to create the owner-only App and install it on this selected repository,
   acknowledging both the logs/artifacts read scope and App-key installation authority. Keep other
   repository/account permissions off, webhook delivery inactive and user authorization unused.
   Record the exact client and installation IDs from the owner's GitHub UI; these may be shared.
2. Follow the single
   [reader bootstrap order in TRUST.md](TRUST.md#explicit-secrets-manager-reader-boundary):
   approve/deploy the disabled stack with empty App parameters, create an empty external secret,
   derive the actual reader roles from its captured outputs, and approve/attach/read back the
   mandatory deny boundary **before importing a PEM**. Select a separate recovery role outside the
   production stack. Review the full nine-role/resource inventory and immutable artifact pins; this
   sequence does not replace approval of the wider deployment.
3. Only after that empty-secret policy verification, approve the secure key import and complete App
   configuration. Record the secret ARN, suffix, App key ID and public-key SHA256 fingerprint, never
   the PEM. Bind the configured suffix to that exact protected secret and verify resolved grants
   while runtime/admission/publication remain disabled. The application needs no OAuth client
   secret. Never put a PEM in chat, command arguments, CloudFormation parameters or source. Role
   replacements require the same approved policy rebinding before use. App setup, empty-secret
   creation, policy attachment, PEM import and live IAM deployment remain separate live approvals.
4. Approve the exact **no-worker** probe mechanism below after the disabled deployment and App
   configuration review. Production handlers cannot establish this evidence with feature flags off
   and reserved concurrency zero. Nor does enabling the normal handler with a disabled server policy
   help: the early policy gate rejects before authenticated GitHub reads. Keep those gates intact;
   do not dispatch a benchmark or enable timers as an authentication test. **F13 remains open until
   the separately approved live acceptance gate passes**.

### No-worker App and IAM acceptance

First review the resolved identity/resource policies and the mandatory attached secret deny policy.
IAM simulation can check expected AWSCURRENT, omitted-stage, VersionId-only, AWSPREVIOUS,
other-secret and unrelated Secrets Manager/KMS outcomes. Record all supplied context and unsupported
policy types; simulation is not proof of live service authorization and cannot substitute for
exercising each actual reader role. The `SimulatePrincipalPolicy` API documents resource-policy
evaluation for IAM users; do not present a supplied secret resource policy as proof of effective
reader-role authorization. An operator's own successful secret read is not evidence for either
Lambda role.

The proposed live mechanism is two temporary functions:
`rcp-production-benchmark-f13-admission-probe` using the exact deployed `AdmissionRole`, and
`rcp-production-benchmark-f13-collector-probe` using the exact deployed `CollectorRole`. Resolve
their role ARNs after disabled stack creation; do not invent physical IDs or add
operator-assume-role trust. A separate approval must name the reviewed hash-pinned probe ZIP and
immutable function versions, invocation principal, both exact role ARNs, fixed non-secret targets,
expiration, costs and cleanup. It must cover exact-function
creation/configuration/invocation/deletion and `iam:PassRole` on only those two roles for
`lambda.amazonaws.com`; it grants no new operator permissions by itself.

Keep `RuntimeEnabled=false`, `AdmissionEnabled=false`, `PublicationEnabled=false`, all production
Lambda concurrencies zero, both production rules disabled, submission/publication denied, and policy
absent or disabled. Require no active production owner or uncertain allocation before probing. Do
not change another controller's live guards. Each probe uses fixed code and empty input to read the
exact external secret and fixed GitHub proof endpoints directly, not through the disabled production
handler. Use the existing admission or collector log group, respectively; default probe-name groups
are outside these roles' log permissions. Do not expand logging access.

Use a 45-second probe timeout and the reviewed provider's bounded observations. Allow concurrency
one only for one synchronous invocation per role, then zero; use no event source, function URL,
resource invoke policy, destination, automatic client retry or returned log tail. Reconcile an
uncertain invocation before any retry. The probe returns only sanitized case outcomes and non-secret
identity, scope and quota metadata. It must never return or log PEM, JWT, token, secret responses or
headers.

Pair each role's successful AWSCURRENT mint/read with explicit `AccessDenied` tests for omitted or
other stages and VersionId-only access. A missing stage/version is not an IAM denial; if a required
fixture does not exist, mark that case unproven and request the exact fixture approval. Another
secret test needs an approved harmless fixture. Do not create a version/secret or widen permissions
just to make a test pass. Verify App visibility is **Only on this account**, webhook delivery is
inactive, and installed permissions and repository selection match the approved dedicated App.
Record the manual checks separately from token-scope evidence. A short probe does not prove real
one-hour token expiration, every other role's effective denial, or instance-origin worker access.

These same-role probes are **not IAM-enforced read-only**: admission can mutate reservation state
and start the state machine, while collection can retire ownership. Reviewed probe code must perform
only the fixed read/negative cases. The disabled coordinator prevents an accidentally started
execution from launching, but state changes and service charges are still possible. Verify no new
execution, active pointer, run/accounting mutation or worker exists afterward. An unexpected change
holds activation until reconciled; do not release ownership merely to clean up a probe. Delete only
the two approved temporary functions after invocation reconciliation and retain sanitized evidence.

The fixed-binding adapters and offline package/template commands are documented in
[ACCEPTANCE.md](ACCEPTANCE.md). They prepare reviewable probes, not deployment authorization or an
automatic acceptance decision.

This disabled-state canary verifies reader access, not an operating watchdog. Independent watchdog
health is a later, separately approved runtime-with-admission-closed check before any paid request.
S3/DynamoDB no-worker canaries and eventual worker-origin/shutdown acceptance remain separate gates.

### Routine rotation, compromise and decommissioning

Choose an owner and review date because App private keys do not expire automatically. Routine
rotation, when no compromise is suspected, needs its own approved overlapping key, secure AWSCURRENT
update, fresh mint/read verification and old-key retirement. Moving AWSCURRENT or deleting a signing
key does not prove previously issued installation tokens revoked. Never fall back to anonymous
reads.

For suspected compromise, obtain approval for the exact incident actions and preserve evidence:

1. Close the separate admission gate and have the installation owner suspend the exact App through
   the GitHub owner UI. A suspension made by the installation owner in the UI cannot be undone by a
   signed App JWT; preserve that distinction from App-manager/API suspension. Preserve ownership,
   immutable shutdown schedules, independent watchdog scans and all allocation/producer deadlines.
   Do not blank App parameters by turning all runtime off: that would disable guards needed by an
   active run. Collection may hold while access is contained; that does not authorize another worker
   or an ownership reset.
2. Contain AWS access **before storing a replacement key**. The baseline secret policy already
   denies readers outside the selected roles. Review that policy and tighten it with a separately
   approved explicit `Deny` for `secretsmanager:GetSecretValue` to the affected reader role ARNs, or
   a reviewed deny-except recovery-principal boundary if incident scope requires it. Preserve
   unrelated statements, use `BlockPublicPolicy`, and verify the effective denial with approved
   same-role evidence. Closing admission or zeroing Lambda concurrency alone leaves the secret
   grants in place. Do not claim account-wide exclusivity from the template's two allows.
3. Quarantine compromised reader functions and revoke affected role sessions under the incident's
   separate approval, without disabling the watchdog or termination role. Session revocation also
   denies other AWS operations for those sessions and does not stop a compromised function obtaining
   fresh credentials. Inspect CloudTrail secret access/policy/version events, repair the compromise
   source and verify trusted code and role configuration before restoring either reader.
4. Only after containment is established, revoke affected GitHub keys and generate/import the
   approved replacement through a trusted recovery identity. Keep the installation suspended and
   secret denial in place until the repaired readers and canary are approved. Restore only the exact
   needed access, verify a fresh mint/read, then have the owner resume the installation. Removal of
   a deny, restoration of a role session path or installation resumption is a separate security
   action, not automatic rollback. A copied PEM remains usable until GitHub revokes that key; AWS
   denial does not erase it. Never assume key rotation invalidates an already-issued token.

If a separately approved KMS signing design replaces readable PEM storage, its incident procedure
must instead specify exact signing-key denial/disablement and cached-token handling. This document
does not provision or approve that alternative or any live policy/recovery identity. The current
Secrets Manager design requires the deny boundary in TRUST.md; it is not an optional hardening step.

For final decommissioning, obtain a separate approval listing the exact App/installation/key IDs,
secret ARN, reader policies, retained collector resources and evidence retention:

1. Close admission, finish physical teardown and all fixed late-producer/allocation reconciliation,
   retain final result/accounting evidence, and verify the active owner is retired. Preserve guards
   until those conditions hold; if compromise makes collection unavailable, use the explicit
   incident recovery plan rather than deleting the ownership record.
2. Disable the now-idle runtime and collector timer/functions. Remove and verify both exact App
   secret-read policy resources, including any residual grant on retained `CollectorRole` after a
   failed stack cleanup. Explicitly account for temporary probes and outstanding reader sessions.
3. Have the owner revoke **every key of this dedicated App**, uninstall its exact installation and,
   if approved, delete the unused App registration. These are separate from removing AWS grants.
   Verify the intended GitHub targets; never affect another build/test App.
4. Schedule deletion of the exact external secret with the approved recovery period (at least seven
   days), after confirming no recovery consumer needs it. Scheduling deletion makes it inaccessible
   immediately; do not use force deletion or silently delete old versions. Under the same explicit
   plan remove retained collector rule/permission, function and role resources once their recovery
   purpose has ended. Preserve logs, retained results, ownership tombstones and budget history for
   their agreed retention. Stack deletion alone does not remove the external secret or GitHub key.

No rotation, containment, revocation, resource deletion or live canary is authorized by this
runbook.

References:
[App token scope and lifetime](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-an-installation-access-token-for-a-github-app),
[App installation authority](https://docs.github.com/en/rest/apps/apps#delete-an-installation-for-the-authenticated-app),
[key rotation](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/managing-private-keys-for-github-apps),
[installation suspension](https://docs.github.com/en/apps/maintaining-github-apps/suspending-a-github-app-installation),
[secret access](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html),
[secret condition keys](https://docs.aws.amazon.com/service-authorization/latest/reference/list_secretsmanager.html),
[secret resource policies](https://docs.aws.amazon.com/secretsmanager/latest/userguide/auth-and-access_resource-policies.html),
[role session revocation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_revoke-sessions.html),
[secret deletion recovery](https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_delete-secret.html),
[Lambda execution roles](https://docs.aws.amazon.com/lambda/latest/dg/lambda-intro-execution-role.html),
[custom Lambda log groups](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-cloudwatchlogs-loggroups.html),
[IAM simulator API limitations](https://docs.aws.amazon.com/IAM/latest/APIReference/API_SimulatePrincipalPolicy.html).

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
   timer supplements the independent allocation-start deadline. Production requires the
   `production-mailbox-sha256-v1` image contract: both worker outputs must carry explicit stored
   SHA-256 checksums. Rebuild from the final reviewed source; images from earlier PR revisions that
   only supplied SDK-default CRC32 are incompatible. The installer checks the reviewed worker digest
   and writes a root-owned, read-only contract marker. The pinned startup checks both that marker
   and the actual worker bytes against `OUTPUT_WORKER_SHA256` before starting any fixture or copy.
   Copying the marker into an old image cannot satisfy the digest check. A mismatch powers off and
   retains the independent cleanup guard; this boot check does not prevent initial EC2 charges.
   Capture provenance must still bind the AMI, exact source, snapshot and numeric launch template; a
   local marker or AMI tag alone is not image attestation. Future `worker.py` changes require
   explicit digest review, tests and a fresh image/launch-template version before activation.
3. Review and seed the disabled policy plus existing cumulative ledger. Select an explicit
   development allowance, finite nonzero monthly allowance and run-rate limit; these are required
   even for the first manual acceptance. A recurring operating budget and schedule remain separate
   decisions. Never initialize missing historical reservations to zero or silently refund them.
4. With separate approval, run a tiny **no-worker** S3/IAM canary. Verify conditional CopyObject
   with `AnnotationDirective=EXCLUDE` (no annotation read/write grants), sealed-version reads,
   create-only writes and the full mixed DynamoDB admission transaction, including rejected direct
   writes and immutable-field mutations. Positive instance-origin IAM verification needs genuine
   EC2-origin credentials and belongs in a separately scoped worker acceptance check; a fabricated
   policy context does not establish it. AWS's CopyObject API and conditional-write guide disagree
   about conditional copy support; the DynamoDB transaction example and general condition-key typing
   also differ. Offline SDK/mocked tests cannot settle live enforcement. Fail closed if the tested
   operation is unsupported; do not remove safeguards.
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
   including cancelled/failed producer recovery. Prepare the exact read-only reporting role and
   non-secret `RCP_AWS_PRODUCTION_PUBLISH_ROLE` / `RCP_AWS_PRODUCTION_TABLE` variables under
   separate approval; reporting shares the production region/bucket variables. Approve the
   `PublicationEnabled` deployment gate and a single keyless reporting acceptance separately. Reuse
   a retained qualification: verify exact frozen records, allowed/denied reads, no AWS writes or
   workers, complete directory rendering and the expected Pages deployment. Restore disabled gates
   after acceptance unless recurring read-only reporting is explicitly authorized. Preserve the
   legacy writer/canary flags false. Broader comparisons, operating allowances and recurring paid
   activation are separate decisions; a reporting success is not paid-run acceptance.

### Recurring orchestration

The benchmark cron is Monday and Friday at 08:17 UTC. It does not select approved operating slots or
authorize spending by being present in YAML. Scheduled builds need both
`RCP_AWS_PRODUCTION_ENABLED=true` and `RCP_AWS_PRODUCTION_SCHEDULE_ENABLED=true`, owner actor ID
`483465`, main and attempt 1. The early credential-free CLI rejects other events, actors, refs, runs
and caller-selected scheduled revisions. A scheduled build uses the triggering main SHA once for
both A/A sides; it receives no OIDC, environment secret or shared candidate cache. Submission still
passes the original signed identity, authenticated GitHub run metadata and transactional admission
checks. No PR, push, fork, comment or `workflow_run` event is a paid trigger.

The server policy is independently required. `scheduled_baseline` accepts either its existing
40-character approved SHA, or the explicit server-only tagged value `triggering-main-aa`. Existing
SHA policies retain their old semantics and never follow a changed main. The tagged value means both
baseline and candidate must equal the signed triggering SHA, with identical binary digests; it is an
A/A trend series, not A/B coverage. An empty baseline, empty slots, unknown tag, nonowner scheduled
actor or out-of-slot run rejects admission. This choice introduces no new policy field, implicit
default or migration of retained payloads. Changing it requires a new reviewed policy version;
nothing in this workflow writes policy, prices, counters or allowances.

Approve exact UTC server slots, initial/total/monthly reservation limits and a daily run limit
before activating either gate. Successful collection still does not refund full reservations.
Preserve the seeded cumulative ledger, slot tombstones and one active owner; do not zero accounting
for a new month or to make an attempt fit. The development ceiling does not establish an ongoing
monthly operating allowance. Price evidence still expires after 31 days. No controller or workflow
gets automatic policy-edit or credential-refresh authority.

The separate keyless reporting workflow declares a daily 09:43 UTC poll behind
`RCP_AWS_PRODUCTION_PUBLISH_ENABLED` and the independent
`RCP_AWS_PRODUCTION_PUBLISH_SCHEDULE_ENABLED=true`. It reads retained frozen reports and never
launches work. Main-only owner identity, exact artifact/inventory bindings and complete-scan
requirements still apply. Its `reader-test` remains manual-only. Read-only schedule activation and
its operating allowance still require approval; this code neither changes live flags nor enables
recurring benchmarks. No history writer key or per-report environment approval is needed.

A scheduled submission job can finish many hours before the cloud collector retires ownership. The
daily reporting scan revisits retained runs rather than treating the submit job conclusion as
workload completion. Runtime/watchdog/collector recovery remains independent of desktop sessions;
local operator restart, an assumed provider expiry or another dispatch cannot replace missing
retirement proof. Both repository schedule gates are absent/off by default. This PR dispatches
nothing, grants no live permission and does not initialize or alter the current closed ledger.

### Manual identity-only workflow acceptance

The existing **AWS production benchmark** workflow has an explicit `identity-test` manual mode. It
excludes validation, candidate building and submission, even if production is accidentally enabled.
The identity job itself requires production's repository variable to be absent or exactly `false`;
it never changes that variable or any AWS setting. Ordinary `benchmark` mode retains the existing
enabled-variable gate. Unknown or missing modes run neither path.

Merging this code does not approve dispatch. Before an explicitly approved single live run, verify
the deployed submit role's immutable ID and trust, production disabled/idle state, the exact
reviewed merged controller SHA, and a recovery window. Use only owner `wykurz` (ID `483465`), branch
`main`, attempt 1. The role is pinned in `acceptance_oidc_workflow.py` to
`arn:aws:iam::381183037896:role/rcp-production-benchmark-GitHubSubmitRole-hg2kGsuS3Qa1`, immutable
ID `AROAVRQCHSHEKT2CRHFFR`, account `381183037896`, region `us-east-1`. A role replacement requires
another reviewed pin update; no role/trust is created or widened by this workflow.

Select `mode=identity-test`, leave `revision` blank, and supply `identity_approval` as a JSON object
with exactly these five fields:

| Field                     | Required value                                                                      |
| ------------------------- | ----------------------------------------------------------------------------------- |
| `schema`                  | `rcp-oidc-workflow-approval-v1`                                                     |
| `source_commit`           | Actual reviewed merged 40-character SHA for this run, never a branch name           |
| `not_before`              | Approved Unix timestamp, integer                                                    |
| `expires_at`              | Approved Unix timestamp, integer; after now and at most one hour after `not_before` |
| `approval_capture_sha256` | SHA256 of the separately retained approval/preflight record                         |

There must be more than 45 seconds remaining after the checkout and dependency installation. No
runnable sample with synthetic times or IDs is provided. The hash is an audit reference, not a
signature, bearer capability or proof of approval. Only the separately approved operator dispatch
authorizes live use. Attempt 1 prevents job reruns; it does not prevent the owner from manually
creating a different run, so reconcile an uncertain run before any new dispatch. Actual run ID is
taken from the runner and compared with both signed tokens.

The five-minute job uses a fresh GitHub-hosted Ubuntu runner, `cache-mode: none`, read-only checkout
without persisted credentials, and hash-pinned Python dependencies installed before token
acquisition. The approved SHA must equal `GITHUB_SHA`, `GITHUB_WORKFLOW_SHA` and clean checkout
HEAD. No environment is attached: an environment would change the subject away from the existing
main-ref trust. The job has only `contents: read` and `id-token: write`, and no repository secret,
App key, candidate artifact, binary execution, release permission or production client invocation.

One process obtains at most two OIDC JWTs, one for the deployed admission audience
`rcp-production-benchmark-admission` and one for `sts.amazonaws.com`. The runner-provided token
endpoint must be HTTPS beneath `actions.githubusercontent.com`; userinfo, fragments, other ports,
redirects, proxies, duplicate audience selection, oversized or incomplete responses fail closed. A
future GitHub endpoint-domain change requires review, not a permissive fallback. Tokens are passed
directly in memory to `acceptance_oidc.run_probe`; they never become command arguments, exported
environment values, files or logs. The runner's built-in request bearer necessarily exists in its
environment, but is not forwarded to the checkout-verification subprocess. Memory erasure is not
guaranteed.

The 45-second process timer covers token retrieval, JWKS verification, the single STS exchange and
`GetCallerIdentity`; cancellation and errors produce only the sanitized hold observation. There are
no request retries. STS requests a 900-second session with an explicit deny-all session policy.
`GetCallerIdentity` still works under that deny and verifies the exact immutable role. Cancellation,
deleting the run or completing the job does not revoke an already issued session; retain its
possible 15-minute validity as part of incident reconciliation.

The observation adds only fixed `diagnostics.stage` and `diagnostics.provider` labels. The stage is
the operation being checked when an error or cancellation occurs, not proof of that operation's
completion or a negative IAM test. Provider labels distinguish the documented issuer URL, the exact
account-bound GitHub provider ARN, another value, an absent value, and a provider not yet
classified. Raw provider values, response objects, tokens, credentials and exception messages never
enter these diagnostics. Cancellation retains the last label while still returning a failed hold. In
particular, `not-observed` can follow a successful STS exchange if the deadline check or
cancellation interrupts before provider classification; it does not prove that no session was
issued.

STS provider identity accepts only `https://token.actions.githubusercontent.com` or
`arn:aws:iam::381183037896:oidc-provider/token.actions.githubusercontent.com`. JWT issuer, audience,
subject, exact role/session and all existing identity checks are unchanged. The
[STS API reference](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html)
describes the issuer form; a [raw SDK response report](https://github.com/boto/botocore/issues/2139)
also demonstrates the ARN form. A CloudTrail provider ARN alone does not establish the raw SDK
response or the precise cause of an earlier generic hold. Offline tests cover both response forms; a
separately approved live observation is still required.

Only `oidc-observation.json` is uploaded, with 30-day retention. A success records authenticated
matching main/run/source identity; `live_accepted` remains `false`. It does not prove underlying
resource-policy denials, because the deny-all session masks any resource grants. It does not prove
production admission, paid-run controls, publication-role access or signed negative actor/ref cases.
The publication role stays disabled. Genuine signed negative fixtures require separate reviewed
execution contexts; the verifier's early rejection of an unsigned wrong-ref payload is not live
negative evidence.

References:
[GitHub OIDC claims and token requests](https://docs.github.com/en/actions/reference/security/oidc),
[AWS web identity sessions](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html),
[GetCallerIdentity permission behavior](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html).

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

The disabled scheduled `triggering-main-aa` policy binds both sides to the authenticated triggering
main commit. A frozen fixed baseline is still valid only for its exact manual A/A commit; A/B
comparisons are not supported. Server slots remain empty until exact cadence, counter limits,
notification and operating-budget decisions pass their acceptance gates. No timer or budget is
changed by this reporting work.

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

## Signed different-workflow denial test

The separate **AWS benchmark identity denial** workflow is manual, main-only and owner-only, with no
environment attached. Keeping the existing main-ref subject makes its different signed workflow
display name the negative dimension. It never edits trust, invokes admission, starts a worker or
enables production. Code review and merge do not approve live dispatch.

After separate approval, bind the reviewed merged SHA, exact deployed submit-role ARN/immutable ID
and unchanged trust, a finite token-eligibility interval, and a separately enforced recovery cutoff.
Only attempt 1 is eligible. The actual role trust compares the workflow display name; it does not
bind an immutable workflow path or source hash. A same-named workflow could meet that AWS condition.
Production admission has its own exact workflow_ref validation.

The nonsecret identity_approval object requires exactly:

| Field                   | Required value                                                           |
| ----------------------- | ------------------------------------------------------------------------ |
| schema                  | rcp-oidc-negative-workflow-approval-v1                                   |
| source_commit           | Reviewed merged SHA, matching workflow SHA, event SHA and clean checkout |
| not_before              | Approved positive Unix timestamp                                         |
| expires_at              | After now, at most 900 seconds after not_before                          |
| recovery_cutoff         | At least expires_at + 1860, at most not_before + 7200                    |
| approval_capture_sha256 | Hash of separately retained actual approval/preflight evidence           |

The 1,860-second reserve covers the maximum 600-second token lifetime, AWS's 300-second OIDC
acceptance skew, a requested 900-second session and 60 seconds for reconciliation. Check actual
operator coverage against that bound before dispatch; successful authentication is not credential
expiry proof. Never generate runnable approvals with synthetic timestamps.

One five-minute job and a 45-second process obtain one STS-audience JWT under the reviewed code. The
job-level OIDC capability also exists in other trusted steps; GitHub does not enforce a one-token
quota. Checkout is read-only at the workflow SHA, dependencies are hash-pinned wheels, caches are
disabled, and no candidate artifact or repository secret is executed or read.

The exact JWT must pass independent GitHub JWKS/RS256 verification and fixed signed
repository/owner/actor/main/run/source/workflow-name checks before a single regional
AssumeRoleWithWebIdentity request. Production's public verifier still rejects the STS audience; the
separate private test verifier shares only the bounded signature/claim implementation. The STS
request always includes a 900-second duration and explicit deny-all session policy.

Only operation AssumeRoleWithWebIdentity, code AccessDenied, HTTP 403 and a canonical request UUID
count as a successful denial. Expired or invalid tokens, provider rejection, throttling, transport
failure, missing evidence and unexpected issuance are holds. There are no SDK retries. A denied
token is never authenticated merely because AWS rejected it.

A checked metadata-only runner journal is fsynced before the possible STS send and after its known
response. The artifact contains only oidc-negative-observation.json and oidc-negative-journal.jsonl,
retained for 30 days. No JWT, credential value, service body, raw exception or token-request
capability enters those files. Tokens and any returned credentials stay in process memory; memory
erasure is not guaranteed.

If a session is unexpectedly issued, preserve only validated expiration/expected-identity metadata
and fail. The horizon is at least token expiry plus AWS skew, session duration and margin. A longer
valid observed expiration extends it instead of being discarded; coverage_exceeded requires renewed
operator recovery coverage and never permits silent closeout. Cancellation may interrupt before
metadata capture, so uncertainty is armed before the send. Completing/cancelling a run does not
revoke a session. The signed session is never used for resource operations.

Runner-local fsync is not external durability. Artifact upload can fail or be cancelled. Keep a
separate operator dispatch intent, exact run ID and approved worst-case recovery cutoff. Missing or
partial artifacts never establish no issuance and never authorize a rerun. Reconcile the exact
request/run and possible-session horizon, then verify production remains disabled and idle.

A passing observation proves this signed different-name context was rejected, not negative
actor/ref/audience cases, same-name path isolation, or whole-system acceptance. The live_accepted
field remains false. The separate image, instance-origin IAM and end-to-end gates still apply.

References:
[AWS GitHub claim mapping](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_iam-condition-keys.html),
[AWS OIDC skew](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html),
[STS sessions](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html).

## Additional file-set coverage

The recurring schedule remains 1 GB-file metadata A/A. Manual 100 MB qualification support is
prepared separately; it is not enabled by merging the workflow choice. The server's `manual_suites`
allowlist defaults to 1 GB only, including when reading existing policy payloads that predate that
field. Scheduled admission independently requires the 1 GB suite. No runtime, budget, credential or
access setting changes automatically when this code lands.

See [incremental 100 MB qualification](LARGE_WORKLOADS.md#incremental-100-mb-metadata-qualification)
for the fixed, explicitly unmeasured qualification budget, image/controller compatibility, live
approval sequence and the separate million-file readiness limits. Qualify on a non-scheduled day;
keep the existing daily-one, single-owner and cumulative-reservation controls intact.

### Extended full-file-set commissioning

The manual workflow also recognizes 1 MB, 1 KB and all-filesets metadata profiles. Default server
authority and scheduled requests remain 1 GB only. The combined profile needs both its explicit
manual_suites opt-in and manual_runtime_seconds=28800; absent extended-runtime fields in recorded
policies mean zero, not an automatic upgrade. It is immutable-owner-only even when ordinary manual
maintainers are configured. Ordinary runtime, capacity, schedule slots and reservations stay
unchanged. Full-set admission derives its two-hour capacity, eight-hour allocation and fixed $50
reservation from the selected immutable contract; a price above that reservation is rejected.

Before any live attempt, review the rebuilt worker, startup/hash/launch bindings and exact
instance-only runtime-profile tag permission. The tag carries a bounded runtime profile, not
authority to ignore the ledger, one-worker rule or budget. Guest and independent expiry guards
remain required. Select a non-Monday/Friday day with the full reconciliation and cleanup window
clear of the next regular slot, authenticate with sufficient recovery coverage, and retain the same
durable one-shot journal through dispatch, collection and retirement. Restoring the experimental
opt-in requires an operator write; it has no automatic expiry.

The original ASG-era RuntimeRole source is retired with deny-only trust and a single explicit
DenyAll permission policy. Applying that source is a separate targeted legacy-stack operation: never
delete the old stack, because production still uses its launch template and security group. The
unused writer App and ruleset bypass also require their separately reviewed retirement; this source
does not delete registrations, keys, retained results, images or shared resources.

New extended admissions also require a canonical UTC `extended_manual_not_after` timestamp. The
service rejects missing or expired windows before reading App-backed run proof and rechecks the
final immutable request creation time. The deadline limits new reservations; it does not cancel an
already admitted run or move its capacity/runtime deadlines. Expired temporary policy remains
readable and regular scheduled 1 GB admission is unchanged. Preserve the admitted policy version
through upload, sealing and retirement; restoring the opt-in with a new version fences stale
requests.

Derive the expiry from authenticated recovery coverage before dispatch. The native 10h15m horizon
starts at the retained request creation time, after workflow build/queue delays. Include cleanup and
mutation margins; fresh credentials at dispatch alone do not guarantee coverage. A queued workflow
that misses the server expiry must not receive a new reservation, even if the operator disconnects.
