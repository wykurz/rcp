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
fields are required in its canonical stored payload. Pricing must have evidence no older than 400
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

The benchmark cron is Monday at 08:17 UTC, once a week. It does not select approved operating slots
or authorize spending by being present in YAML. Scheduled builds need both
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
monthly operating allowance. Price evidence still expires, after 400 days. No controller or workflow
gets automatic policy-edit or credential-refresh authority.

The separate keyless reporting workflow declares a daily 09:43 UTC poll and a Tuesday 03:43 UTC
post-drain refresh behind `RCP_AWS_PRODUCTION_PUBLISH_ENABLED` and the independent
`RCP_AWS_PRODUCTION_PUBLISH_SCHEDULE_ENABLED=true`. It reads retained frozen reports and never
launches work. Main-only owner identity, exact artifact/inventory bindings and complete-scan
requirements still apply. Its `reader-test` remains manual-only. Read-only schedule activation and
its operating allowance still require approval; this code neither changes live flags nor enables
recurring benchmarks. No history writer key or per-report environment approval is needed.

A scheduled submission job can finish many hours before the cloud collector retires ownership. The
reporting scans revisit retained runs rather than treating the submit job conclusion as workload
completion. The Tuesday refresh follows the regular Monday 08:17 UTC slot, even with the maximum
one-hour policy lateness, two-hour accepted workflow age, the two-hour capacity search of a
full-matrix plan, its allocation of eight or twelve hours and 15-minute drain, plus more than two
hours for collection after the longer one. This is a reporting opportunity, not a promise that
recovery has completed: outages can leave a pending/held observation, and the daily poll or an
existing manual publish refresh can observe later progress.

Before retirement, the snapshot and Actions summary also show an allowlisted controller-recorded
worker outcome, safe close category, fixed deadlines and cleanup stage. A recorded worker success is
provisional; only a retirement-bound validated report establishes a benchmark result. Cleanup
timestamps describe recorded controller verification, not a new EC2 query or proof of current
physical state. Missing, invalid or foreign committed status is shown as unavailable; raw status
messages, logs, credentials and AWS identities are not copied into the public progress fields. These
observations reuse the existing active/run GetItem permission and add no AWS service reads, IAM
grants, worker invocations or controller deployment. The frozen measurement records and their
retirement checks remain unchanged.

Runtime/watchdog/collector recovery remains independent of desktop sessions; local operator restart,
an assumed provider expiry or another dispatch cannot replace missing retirement proof. Both
repository schedule gates are absent/off by default. This PR dispatches nothing, grants no live
permission and does not initialize or alter the current closed ledger.

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
field. Scheduled admission independently requires the policy's `scheduled_suite`, which is the 1 GB
suite unless a policy version names another one (see [Benchmark matrix](#benchmark-matrix)). No
runtime, budget, credential or access setting changes automatically when this code lands.

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
remain required. Select a day with the full reconciliation and cleanup window clear of the regular
Monday slot, authenticate with sufficient recovery coverage, and retain the same durable one-shot
journal through dispatch, collection and retirement. Restoring the experimental opt-in requires an
operator write; it has no automatic expiry.

The original ASG-era RuntimeRole source is retired with deny-only trust and a single explicit
DenyAll permission policy. Applying that source is a separate targeted legacy-stack operation: never
delete the old stack, because production still uses its launch template and security group. The
unused writer App and ruleset bypass also require their separately reviewed retirement; this source
does not delete registrations, keys, retained results, images or shared resources.

New extended admissions also require a canonical UTC `extended_manual_not_after` timestamp. The
service rejects missing or expired windows before reading App-backed run proof and rechecks the
final immutable request creation time. The deadline limits new reservations; it does not cancel an
already admitted run or move its capacity/runtime deadlines. Expired temporary policy remains
readable and regular scheduled admission is unchanged. Preserve the admitted policy version through
upload, sealing and retirement; restoring the opt-in with a new version fences stale requests.

Derive the expiry from authenticated recovery coverage before dispatch. The native 10h15m horizon
starts at the retained request creation time, after workflow build/queue delays. Include cleanup and
mutation margins; fresh credentials at dispatch alone do not guarantee coverage. A queued workflow
that misses the server expiry must not receive a new reservation, even if the operator disconnects.

## Benchmark matrix

The matrix suites measure the build under test, with rcp's default settings, on several file sets in
one allocation. A *cell* is one file set under one scenario. Each cell is three copies made by one
build. There is no baseline build, no pairing and no verification of what was copied: a copy counts
when rcp exits zero and reports the expected file count. CPU and memory are what rcp reports about
itself in its JSON summary; the `revision` input may still name a revision from before rcp could
print one, which is then timed without those figures.
[The matrix runner](../../docs/benchmarking.md#performance-only-matrix-runs) describes what a run
promises, and [the worker profile](LARGE_WORKLOADS.md#performance-only-matrix) how it runs on AWS.

There are two scenarios. `local` copies between the two arrays. `remote` makes the same copy through
rcp's remote path on the one worker: rcp starts a source and a destination `rcpd` over SSH, and the
daemons exchange the data over loopback TCP with rcp's default encryption. No network hardware and
no added latency are involved; it measures the remote protocol, not a network.

Six more scenarios time the other tools. A tool cell is named after the copies it follows and its
tool: `local-rcmp`, `local-rlink` and `local-rrm` run on every copy of the same file set's local
cell, and `remote-rcmp`, `remote-rlink` and `remote-rrm` on every copy of its remote cell. After a
copy the tools run in that order: compare the copy with its source by file size, hard-link it into a
new tree beside it, and remove it. A tool cell is three runs of its tool and counts as a copy does,
on its exit status and the counts of its own summary. No cache is dropped for a tool. A run with an
`rrm` cell also clears every other tree with that `rrm`, unmeasured, and falls back to `rm -rf` if
`rrm` fails at one. [Tool cells](../../docs/benchmarking.md#tool-cells) says what each one measures.

Two more copy scenarios keep their trees on other storage, each with its three tool cells
(`nfs-rcmp`, `memory-rrm` and so on):

- **`nfs`** copies between two mounts of a network filesystem. The supervisor serves the two arrays
  with the kernel's NFS server on the loopback and mounts them back with NFS 4.2, with 2.5 ms added
  to every packet on that loopback. A request to the server and its reply then cost 5 ms, as an
  operation on a file that is not cached does on a filer. The server answers without waiting for its
  disks (`async` exports), runs 256 threads and hands out no delegations. The client's caches are
  dropped before every copy and before every tool run. Only what is measured goes through the
  mounts: the file set is generated on the array itself.
- **`memory`** copies between two filesystems in memory (tmpfs, 64 GiB each), where no operation
  costs I/O. No cache is dropped there.

[Cells on other storage](../../docs/benchmarking.md#cells-on-other-storage) says how the runner
treats them. The supervisor prepares this storage before any candidate process exists
(`other_storage.py`) and checks it before it trusts it: the daemons run, the server is past its
grace period, the mounts are the loopback exports, and a round trip costs what the delay says. If a
preparation fails, the run still measures every other cell. The cells on that storage are `failed`
at stage `storage` with the reason, and the worker's status says what went wrong (`other_storage`).

A suite is named `matrix-<selection>-<scenarios>`:

| Selection    | File sets                                      | Allocation               | Reservation                 |
| ------------ | ---------------------------------------------- | ------------------------ | --------------------------- |
| `all`        | 1 GB, 100 MB, 1 MB and 1 KB files              | 8 hours, 12 with `tools` | fixed $50, $65 with `tools` |
| `1g`         | 1 GB files                                     | 3 hours                  | computed                    |
| `100m`       | 100 MB files                                   | 3 hours                  | computed                    |
| `1m`         | 1 MB files                                     | 3 hours                  | computed                    |
| `1k`         | 1 KB files                                     | 3 hours                  | computed                    |
| `commission` | two small fixtures (16 files and 10,000 files) | 3 hours                  | computed                    |

| Scenarios                  | Cells of each selected file set                                  |
| -------------------------- | ---------------------------------------------------------------- |
| `local`                    | one local cell                                                   |
| `remote`                   | one remote cell                                                  |
| `local+remote`             | a local cell, then a remote cell, on one generated fixture       |
| `local+tools`              | a local cell, then `rcmp`, `rlink` and `rrm` cells on its copies |
| `local+tools+remote`       | the same four cells, then a remote cell                          |
| `local+tools+remote+tools` | the same five cells, then the three tools on the remote copies   |

The name of a scenario set reads as the order of a file set's cells, and `tools` stands for the tool
cells of the copy cell before it. That is thirty-six suites, for example `matrix-all-local+remote`
(eight cells), `matrix-1m-remote` (one cell), `matrix-commission-local+remote` (four cells on the
small fixtures) and `matrix-all-local+tools+remote+tools` (thirty-two cells). The allocation follows
the selection and, for every file set together, whether the tools are timed: eight hours without
them, twelve with them. The eight cells of `matrix-all-local+remote` took 5 h 32 min of their eight
hours in the first full run. How long the tools take on these arrays is not known until a suite with
them has run, which is why their allocation is generous: the machine is billed for the time it is
up, and it powers off when the run ends, so time that a run does not use is not paid for. Cells that
do not fit are reported `not_run` and the others count; the 1 KB file set is last, and within it the
remote cell and its tools.

Three more scenario sets measure on other storage. They exist for the selections `all` and
`commission` only: a single file set of the arrays has nothing to measure there. A file set has the
cells of the storage it is kept on, so these suites are not every file set under every scenario:

| Scenarios                                         | Cells                                                            |
| ------------------------------------------------- | ---------------------------------------------------------------- |
| `nfs+tools`                                       | an `nfs` cell and its three tool cells, on the NFS file set      |
| `memory+tools`                                    | a `memory` cell and its three tool cells, on the set in memory   |
| `local+tools+remote+tools+nfs+tools+memory+tools` | the eight cells of each file set of the arrays, then those eight |

| Selection    | NFS file set                                                 | File set in memory                                  |
| ------------ | ------------------------------------------------------------ | --------------------------------------------------- |
| `all`        | `tiny-1k-700x1024`: 716,800 files of 1 KB in 700 directories | `tiny-1k-10240k-memory`: the 1 KB files once more   |
| `commission` | `commission-10k`, the small fixture of the arrays            | `commission-10k-memory`: the same fixture in memory |

That is six more suites, forty-two in all.
`matrix-all-local+tools+remote+tools+nfs+tools+memory+tools` has forty cells, and the twelve-hour
allocation with its $65 reservation from its first plan version. `matrix-all-nfs+tools` and
`matrix-all-memory+tools` have four cells each and three hours. A network filesystem takes one new
file at a time in a directory, so its file set is many small directories. At 5 ms a request, a copy
of it is estimated at five minutes and each tool at about one, from runs in a virtual machine: no
worker has measured it yet. The file set in memory has an id of its own because a file set is
generated once, on one storage.

Twelve hours are a second runtime profile, `all-filesets-12h-v1`, beside `all-filesets-8h-v1`. A
profile is one exact runtime, and every place that bounds a run holds it: the plan, the tag that the
coordinator may put on the instance, the expiry scan that reads that tag, the extension that the
worker asks its supervisor for, and the sampler, whose journal covers the longest run. The
reservation of a twelve-hour run is fixed at $65: at $4.608 an hour, with sixteen minutes of
shutdown and the ancillary allowance, twelve hours compute to $61.47. The three suites were first
given eight hours. That plan stays recorded as their version 1, so anything admitted under it is
still read, and the twelve-hour plan is their version 2.

A computed reservation is the ordinary three-hour one: $20 at the current price and ancillary
allowance. A suite name selects exactly one fixed plan, as before; there is no free-form selection.
The request keeps its two-sided shape (identical baseline and candidate revisions, six binary keys
with identical digests), so mailbox, IAM, window and policy-manifest formats are unchanged. The
baseline copies of the binaries are uploaded and never run.

A suite with `+tools` sends six more keys, `rcmp`, `rlink` and `rrm` on both sides, twelve in all.
Each plan names its binaries (`binary_names`), and every step holds a request to exactly those: the
client's manifest, the upload capabilities, the sealed inputs, the worker's mailbox and the tools
the result says it ran. The workflow builds one artifact with all six tools, and the client sends
the part that its suite's plan names. A suite without `+tools` sends and accepts what it always did.

### The weekly suite

`matrix-weekly-2` is the suite made for the weekly run. It measures each thing once, on file sets
small enough that a command takes a minute or two. The suites above are far from that.
`matrix-all-local+tools+remote+tools` ran on 25 November 2026 (`repo-655073755-run-45515239343`) and
measured its thirty-two cells in 8 h 20 min. A copy of a terabyte took 10.4 to 10.7 minutes and a
copy or a tool on ten million small files up to 11.3, while the tools on the sets of large files
were done in 0.0 to 1.5 seconds. The weekly suite has file sets of its own, sized from that run, and
eighteen cells:

| File set               | Files                              | Cells                                               | Each command          |
| ---------------------- | ---------------------------------- | --------------------------------------------------- | --------------------- |
| `bulk-1g-200gb`        | 200 of 1 GB                        | `local`, `remote`                                   | about 2 minutes       |
| `bulk-1m-200gb`        | 200,000 of 1 MB                    | `local`, `remote`                                   | about 2 minutes       |
| `tiny-1k-2048k`        | 2,048,000 of 1 KB                  | `local` and the three tools, `remote`, `remote-rrm` | 0.7 to 2.2 minutes    |
| `tiny-1k-700x1024`     | 716,800 of 1 KB in 700 directories | `nfs` and its three tools                           | no worker measured it |
| `tiny-1k-2048k-memory` | 2,048,000 of 1 KB                  | `memory` and its three tools                        | no worker measured it |

The times of the first three rows are what the run of 25 November gives for these sizes, in
proportion. For the other two there is less to go by. A virtual machine gave about five minutes for
a copy on NFS and about one for each tool. The set in memory is sized as the one on the arrays, on
the expectation that a command on small files costs there what it costs here. On the arrays the
local copy and each tool kept about 80 of the 96 processors busy, nearly all of that time in the
kernel, so what they wait for is not the disks.

- **Large files.** A copy of them takes as long as their bytes take to write, so the sets hold 200
  GB where they held a terabyte. That is about as much as the machine has memory. The kernel lets a
  copy run ahead of the disks by a part of that memory only: the copies of a terabyte ended with a
  few tens of gigabytes still to be written. Most of such a copy therefore still goes at the pace of
  the destination array, and it may take somewhat less than its share of the terabyte's time.
- **No set of 100 MB files.** It gave what the set of 1 GB files gave: 639 and 621 seconds for a
  local copy, 941 and 896 seconds of CPU, and within 3% the same CPU for the two daemons of a remote
  copy. The set of 1 MB files differs, with 1,938 seconds of CPU and five times the memory, and
  stays.
- **Small files.** Everything on them takes as long as their files are many, so the set has
  2,048,000 files where it had 10,240,000, on the arrays and in memory. Its remote copy is the
  shortest command, at 0.7 minutes. The file set on NFS is the one the other suites have: no worker
  has measured it, so its size waits for a first run.
- **Tools.** `rcmp`, `rlink` and `rrm` are timed on the sets of small files only. Per file they cost
  on 1 MB files what they cost on 1 KB files (`rcmp` 65 and 64 microseconds, `rlink` 51 and 56,
  `rrm` 28 and 29), so their cells on large files said nothing more: a second or two on the files of
  1 GB and of 100 MB, and 28 to 68 seconds on those of 1 MB.
- **Each tool once.** A remote copy leaves the tree that a local copy leaves. `rcmp` and `rlink`
  after the remote copies measured the same again, 3.5% slower, and with the removal of what `rlink`
  made they took 89 of that run's 500 minutes. On the arrays they therefore follow the local copies
  only. `rrm` follows every copy: the copy has to go, and its run is what removes it.

About two hours of work are expected, a third of them on storage that no worker has measured. The
suite has the eight-hour allocation with its fixed $50 reservation (`all-filesets-8h-v1`): the bill
follows the time a run takes and not its allocation, and a shorter allocation would be a runtime
profile that the stack does not have. It sends the binaries of a suite with `+tools`. Its name does
not follow the pattern above, because its cells are not every file set under every scenario. The
number in the name is a version: the cells of a suite never change, so another choice of file sets
or scenarios is `matrix-weekly-3`.

`matrix-weekly-1` was defined first, for commands of one to five minutes: 400 GB of large files in
three sets, 4,096,000 small files, and every tool after the local and after the remote copies. It
never ran. It stays as it was recorded, as every suite does, and nothing schedules it; its file sets
have no trend on the pages. That is forty-four suites.

The pages follow this suite once a run of it has measured something. Its file sets are then the ones
that every page shows, and the trends of the file sets before them are kept, collapsed, at the end
of the page of each kind of copy and of each tool; [RESULTS.md](RESULTS.md) says how. Until then the
pages are what they were.

### The reference suite

`matrix-reference-1` says what `cp` and `rsync` take for the copies of the weekly suite, without the
one in memory. It is run by hand, once: those tools do not change from week to week, so their times
stand beside rcp's trend and are no trend of their own. Each file set is copied by rcp first, as in
the weekly suite, and then by each of the two, in eighteen cells:

| File set           | Cells                                                        |
| ------------------ | ------------------------------------------------------------ |
| `tiny-1k-2048k`    | `local`, `local-cp`, `local-rsync`, `remote`, `remote-rsync` |
| `tiny-1k-700x1024` | `nfs`, `nfs-cp`, `nfs-rsync`                                 |
| `bulk-1m-200gb`    | `local`, `local-cp`, `local-rsync`, `remote`, `remote-rsync` |
| `bulk-1g-200gb`    | `local`, `local-cp`, `local-rsync`, `remote`, `remote-rsync` |

- **The commands.** `cp -a` and `rsync -a` of the worker image, between the two places of the copy
  beside them. Beside the remote copies `rsync` pulls the file set over SSH; `cp` has no such copy.
  [Reference cells](../../docs/benchmarking.md#reference-cells) says how the runner makes them.
- **The limit.** A copy by either tool gets ten times the median of rcp's copies of the same file
  set. One that takes longer is cut off and the tool is not tried again there: the pages say that it
  did not finish in that time. On the network filesystem both tools are expected to end this way,
  after about an hour each.
- **Small files first.** There the tools differ; on large files every one of them waits for the same
  array. Should the allocation end early, the cells that are left are the least telling.

Five to seven hours of work are expected, and no worker has measured any of it. The worst case is
longer, so the suite has the longest allocation, twelve hours with its fixed $65 reservation
(`all-filesets-12h-v1`): cells that do not fit are reported as not run, and the bill follows the
time a run takes. `matrix-commission-reference` has the same kinds of cell on the two small
fixtures, to exercise such a run end to end in minutes. That is forty-six suites.

### Select a run by hand

The workflow's `suite` input offers every matrix suite. The server admits one only when the policy's
`manual_suites` names it. That list stays explicit and holds at most eight suites; nothing is
enabled by default or by a pattern. Three-hour suites use the ordinary runtime and the ordinary
manual requesters. The `matrix-all-…` suites need what every longer run needs:
`manual_runtime_seconds`, an unexpired `extended_manual_not_after`, and the immutable owner as
requester. The weekly suites and `matrix-reference-1` need the same. A plan with a runtime profile
runs for exactly that profile's time: `manual_runtime_seconds` is `28800` for the three suites of
every file set without the tools and for the weekly suites, and `43200` for the suites of every file
set with the tools and for `matrix-reference-1`, and no other value is accepted. A policy holds one
such value, so its `manual_suites` can name eight-hour suites or twelve-hour suites next to any
three-hour ones, and not both.

### The weekly schedule

The scheduled run is the whole matrix. On a schedule event, Monday at 08:17 UTC, the workflow asks
for `production_workflow.SCHEDULED_SUITE`, which is `matrix-all-local+remote`: every file set,
copied locally and through `rcpd`. The paired 1 GB run is no longer scheduled. The matrix's first
cell copies the same file set, and its point continues that file set's trend line;
`bulk-1g-metadata` remains a suite that can be started by hand while the policy lists it.
[The weekly suite](#the-weekly-suite), `matrix-weekly-2`, is meant to take this slot once it has run
by hand; [Deploy and commission](#deploy-and-commission) has the steps.

After submission succeeds, a separate 15-minute GitHub Actions job reuses the build for a bounded
local and encrypted-loopback `--preserve-settings=all` benchmark. It receives no cloud credentials
and adds no EC2 work. Its 90-day Actions artifacts are separate from the matrix's published history;
see
[weekly full-preservation coverage](../../docs/benchmarking.md#weekly-full-preservation-coverage)
for fixtures, verification and interpretation.

The server admits a scheduled request only when its policy agrees, in three fields:

- `scheduled_suite` names the one suite a scheduled request may ask for: `matrix-all-local+remote`.
  A policy stored before the field existed keeps its default, `bulk-1g-metadata`.
- `scheduled_runtime_seconds` separately approves the runtime of the suite's profile: `28800` for
  this suite and for `matrix-weekly-2`, `43200` for every file set with the tools. It is rejected
  when the scheduled suite has an ordinary plan.
- `schedule_slots` holds the slot `[0, 8, 17]`: weekday 0 is Monday.

A payload that names only one of the first two fields is not a recorded shape and is refused. A
scheduled run of either profile is still owner-only, needs its matching slot and is bounded by the
daily, monthly and total allowances; it reserves the fixed amount of its plan, $50 or $65. It has no
expiry, because it recurs by design. Neither field lets anyone start the suite by hand, and the
manual opt-in never stands in for the scheduled one. The two approvals are separate fields, so one
policy can keep scheduling the eight-hour suite while a twelve-hour suite is tried by hand.

The server refuses a request whose suite differs from its policy before any reservation, so source
and policy cannot drift silently: under a policy that still names the 1 GB run, a scheduled matrix
request costs nothing and runs nothing. Changing what is scheduled therefore takes both a reviewed
source change (the `SCHEDULED_SUITE` constant, the cron line and `REGULAR_CRON`) and a new policy
version that names the same suite, its slot and its runtime. The policy change needs no image, no
package and no stack update. Keep `RCP_AWS_PRODUCTION_SCHEDULE_ENABLED` off between the two, so that
a slot in between is skipped instead of refused.

### What renews and what does not

A weekly run needs no regular visit to the policy, because its limits are of three kinds:

- The monthly allowance renews by itself. `monthly_cap_microusd` bounds the reservations of one UTC
  calendar month, and every month has its own counter. Nothing carries over.
- The total allowance never renews. `total_cap_microusd` bounds every reservation since the ledger
  was seeded, so it is the last stop when everything else is forgotten. Set it as far out as the
  owner wants to commit, and raise it with a new policy version before it is reached.
- Price evidence is valid for 400 days, counted from `priced_at`. After that the server refuses
  every request, manual or scheduled, before any reservation, until a new policy version carries a
  newly confirmed price.

To confirm the price, read the public on-demand price of the machine and keep the answer:

```text
aws pricing get-products --region us-east-1 --service-code AmazonEC2 \
  --filters Type=TERM_MATCH,Field=sku,Value=SKU --format-version aws_v1 --output json
```

`hourly_microusd` is the price in that answer, `priced_at` is the time it was read, and
`price_evidence` names the SKU and the SHA-256 of the saved answer. A price that pushes a
reservation over the fixed amount of its plan, $50 for eight hours and $65 for twelve, is refused
instead of being absorbed.

`extended_manual_not_after` is separate. It bounds the runs beyond three hours that are started by
hand, not the schedule, and has no effect on how long the schedule keeps running.

### How a run qualifies

The run-level requirements are unchanged: verified cleanup, the approved image and source revision,
an admitted worker and the exact request. The worker's result must hold the suite's contract: the
same cells in order, three copies, the 3600-second hang limit, the cache policy, the runner label,
and measured binaries whose digests are the approved ones. Within that, a matrix run qualifies cell
by cell:

- It counts (`benchmark_valid`) when at least one cell completed. Every cell's outcome, stage and
  short reason are in the collection report and in the public record; only completed cells have
  measurements.
- A benchmark command that was stopped keeps the cells its saved results show as finished. The
  record then carries the note `worker: the benchmark command did not exit cleanly`. Paired suites
  are all or nothing and still require a clean exit.
- A result that differs from the suite's contract publishes neither cells nor measurements, only the
  reason.

Remote cells qualify by the same rules. A remote cell that could not reach its private SSH endpoint
is `failed` at stage `transport`, with the reason, and the run's other cells still count.

Tool cells qualify by the same rules. A tool that fails is `failed` at stage `command` or `summary`,
and the copies and the other tools of the run still count. A tool cell whose copy failed takes that
copy's outcome at stage `copy`: a remote copy that fails costs the tools after it, not the ones
after the local copy. When `rrm` failed while clearing a tree and `rm -rf` took over, the record
carries the note `worker: rrm failed while clearing a tree; rm -rf cleared the trees after that`;
the cells are unaffected.

Cells on other storage qualify by the same rules too. When the supervisor could not prepare a
storage, its copy cell is `failed` at stage `storage` with the reason, its tool cells are `failed`
at stage `copy`, and the run's other cells still count.

Public records of matrix runs are version 4 (`rcp-production-public-report-v4`). Their diagnostics
are version 2, or version 3 when the run has a remote cell, which adds each daemon's CPU and memory.
A run of a suite with tool cells publishes version 5 and diagnostics version 4, where a tool cell is
named by what its tool ran. A run of a suite with cells on other storage publishes version 6 and
diagnostics version 5, which also says what that storage was in the run.
[RESULTS.md](RESULTS.md#matrix-records-and-retained-evidence) describes them. Commissioning runs
qualify like any other, and their small fixtures stay outside the trends.

### Deploy and commission

Nothing here changes a live setting by being merged. The server packages and the worker image must
change **together**. `worker.py` has a new pinned digest; the coordinator compares the launch
template's startup data with the reviewed startup that embeds that digest, and it does so for every
suite. New packages with the old launch template, or the reverse, would admit a run, keep its
reservation and then refuse to launch it. Existing suites, plans and stored policies are otherwise
unchanged.

Remote cells need three things of the image, which a rebuild from this revision provides and checks
([AMI.md](AMI.md)):

- `sshd`, `ssh` and `ssh-keygen` (`openssh-server`, `openssh-client`). The benchmark starts its own
  unprivileged `sshd` inside the measurement unit; the system SSH service is not used and is left as
  the base image has it.
- A benchmark account that `sshd` can run a command for: login shell `/bin/sh` and an existing home,
  the empty root-owned `/opt/rcp-benchmark/home`. The account still has no password and no
  authorized key; the only key that opens it is made per run inside the unit's private `/tmp` and
  goes away with the unit.
- A measurement unit that may open IPv4 sockets. A suite with a remote cell runs with
  `RestrictAddressFamilies=AF_UNIX AF_INET`; every other suite keeps `AF_UNIX`. `PrivateNetwork=yes`
  is unchanged for all of them, so the only interface is the unit's own loopback and nothing leaves
  the unit.

The image build proves all three before the image can become ready: it logs in once through the
private `sshd`, as the benchmark account, in a unit with the sandbox of a remote suite
(`infra/aws_benchmark/remote_check.py`). The login must succeed and print nothing of its own: rcp
reads the first line an `rcpd` prints as its readiness record, so a home that `sshd` cannot enter
would break every remote copy. An image that cannot serve remote cells fails its build with the
reason on the console.

Use the [cutover sequence](#bind-admission-to-a-deployment-during-cutover): between regular slots
and with no active owner, close admission; switch the server packages, the rebuilt worker image, the
launch-template version and the pins as one step; verify them; then reopen with a new policy version
bound to the new pins. That policy version names in `manual_suites` the matrix suites to test, with
the extended manual window when the full matrix is among them. Scheduled requests are admitted once
the policy also names the scheduled suite, its runtime and its slot
([The weekly schedule](#the-weekly-schedule)).

Suites with `+tools` need three things of a deployment, again together:

- **The stack template of this revision.** The worker may read, and the admission and runtime
  functions may stage and copy, object keys for `rcmp`, `rlink` and `rrm` on both sides. Each of the
  seven access lists names its keys one by one, so this is a template change, not a parameter. The
  coordinator may also tag an instance with the twelve-hour runtime profile; without that, the
  launch of every file set with the tools is denied.
- **The server packages of this revision.** Earlier packages know three binaries and refuse a
  manifest with any other. They also know one runtime profile: their expiry scan would end an
  instance tagged with the twelve-hour one at once.
- **A worker image of this revision.** Its runner knows the tool cells and samples a run for up to
  twelve hours, and its `worker.py` fetches the binaries that the plan names and asks its supervisor
  for the time of its plan, which gives it a new pinned digest and the launch template new startup
  data.

Until all three are in place, a request for such a suite is refused at admission, before any
reservation: the deployed policy does not name the suite and the deployed packages do not know it.
The scheduled suite stays `matrix-all-local+remote` in this revision. No suite of every file set
with the tools is meant for the schedule. The run of 25 November 2026 fitted its twelve hours, but
most of its commands took ten minutes, where five say as much, or a second or two, which is noise.
[The weekly suite](#the-weekly-suite) is sized from that run and is the one to schedule; the steps
are below.

Suites with cells on other storage need two things of a deployment, together. They send the binaries
that the suites with `+tools` send, so the stack template is unchanged:

- **The server packages of this revision.** Earlier packages do not know these suites, their file
  sets or the record version they publish.
- **A worker image of this revision.** Its installer adds the NFS server packages with their
  services and sockets masked before they arrive, so that nothing of them starts or listens by
  itself. Its build then proves on the image's own kernel that the supervisor can serve two
  directories over NFS with the delay, mount them, and reach every mount as the benchmark account
  from the measurement sandbox, and takes everything down again (`storage_check.py`, stage
  `verify-storage-cells`). A build that cannot do this fails there. `worker.py` prepares the storage
  and has a new pinned digest, which gives the launch template new startup data. The runner of this
  image samples a run for more phases than before, because the largest suite marks more of them.

Until both are in place and the policy names the suites, a request for one is refused at admission,
before any reservation. This revision does not change the scheduled suite.

The weekly suite needs the same two things, and nothing else: it sends the binaries of a suite with
`+tools` and prepares the storage of a suite with cells there.

- **The server packages of this revision.** Earlier packages do not know `matrix-weekly-2`, its plan
  or its file sets.
- **A worker image of this revision.** The runner on the image reads its file sets from the image's
  own `benchmarks/matrix-cases.json`, and the worker its suites from the image's own catalog.
  `worker.py` is unchanged, so its pinned digest is.

It runs for eight hours at most, so a policy that names it for a run by hand has
`manual_runtime_seconds=28800`. Scheduling it is a further reviewed step, after it has run by hand:
change `production_workflow.SCHEDULED_SUITE` and the policy's `scheduled_suite` to `matrix-weekly-2`
together, with `scheduled_runtime_seconds=28800` as it is today. A scheduled run of the suite before
may come in between. Its points go to the earlier trends, and the pages stay those of the weekly
suite.

The reference suites need two things of a deployment, together:

- **The server packages of this revision.** Earlier packages do not know these suites.
- **A worker image of this revision.** It has `rsync`, and its `worker.py` has a new pinned digest,
  which gives the launch template new startup data.

`matrix-reference-1` runs for twelve hours at most, so a policy that names it for a run by hand has
`manual_runtime_seconds=43200`. A policy cannot have that next to the eight hours of a run of the
weekly suite by hand. The policy version that schedules the weekly suite, or any after it, can name
`matrix-reference-1` instead: a scheduled run has its own runtime.

Commission one run at a time. A run holds the single active owner until its whole horizon has
passed, not until its work is done: the capacity-search window, plus the runtime, plus fifteen
minutes, counted from admission; collection follows. With the default twelve-hour capacity window
that is about 15 h 15 min for a three-hour suite, however briefly it ran, about 10 h 15 min for
`matrix-all-local`, whose plan caps the search at two hours, and about 14 h 15 min for every file
set with the tools. The run's public page appears only after that. A dispatch before then is refused
before any reservation, and a run admitted within that horizon before a scheduled slot displaces the
scheduled run. A lower `capacity_seconds` in the commissioning policy version shortens the horizon.
Size `extended_manual_not_after` for the full run and its repeat from these numbers.

1. `matrix-commission-local+remote`: a few minutes of work that exercises every step of both
   scenarios, including the public pages. Expect four of four measurements, and on the run page the
   resources of `rcp`, the source `rcpd` and the destination `rcpd` for every remote copy.
2. `matrix-all-local+remote` by hand. This run says how long the remote cells take and whether all
   eight cells fit eight hours. If they do not, the choice is between a longer allocation, which is
   a new priced plan, and measuring local and remote cells in separate runs (`matrix-all-local`,
   `matrix-all-remote`). By hand that works as it is; on a schedule it does not, because a schedule
   names exactly one suite. Either way it is a further reviewed change.
3. The same suite once more, to see that two runs agree. The weekly scheduled run can be that
   repeat: it is the same suite.

For the tool cells, after the deployment above:

1. `matrix-commission-local+tools+remote+tools`: a few minutes of work. Expect sixteen of sixteen
   measurements, and on the run page a row for each tool after each kind of copy, under both small
   fixtures, with the tool's own CPU time and peak memory.
2. `matrix-all-local+tools+remote+tools` by hand. This run says how long each tool takes on these
   arrays and whether all thirty-two cells fit twelve hours. It was made on 25 November 2026: every
   cell was measured, in 8 h 20 min.

For the cells on other storage, after the deployment above:

1. `matrix-commission-local+tools+remote+tools+nfs+tools+memory+tools`: a few minutes of work.
   Expect twenty-four of twenty-four measurements. Under *Other storage of this run* the run page
   shows the NFS version, the round trip the worker measured (5 ms and a little more), the added
   delay and the limits of the roots in memory.
2. `matrix-all-nfs+tools` and `matrix-all-memory+tools` by hand, four cells each. They say how long
   a copy and each tool take there, which no worker has measured: the NFS file set is sized from a
   virtual machine and may need another size.
3. `matrix-all-local+tools+remote+tools+nfs+tools+memory+tools` by hand, if all forty cells are
   wanted once. The weekly suite measures the same storage with file sets of its own and does not
   need this run.

For the weekly suite, after the deployment above:

1. `matrix-commission-local+tools+remote+tools+nfs+tools+memory+tools`: the commissioning run of the
   cells on other storage, above. It exercises every kind of cell the weekly suite has.
2. `matrix-weekly-2` by hand. Expect eighteen of eighteen measurements in about two hours, and each
   command on the arrays between forty seconds and a little over two minutes. The cells on NFS and
   in memory are the ones no worker has measured: if one takes seconds, or many minutes, its file
   set needs another size, which is a new file set in `matrix-weekly-3`. From this run on the pages
   show the file sets of the weekly suite.
3. The schedule, as above, once that run is as expected.

For the reference suite, after the deployment above:

1. `matrix-commission-reference`: a few minutes of work. Expect the ten cells on the arrays
   measured. On the network filesystem `cp` and `rsync` copy one file after the other, so their two
   cells may well be cut off at ten times what rcp took: the run then shows that step too, and still
   counts.
2. `matrix-reference-1` by hand, once. Expect five to seven hours of work, and its page about 14 h
   15 min after admission. From then on the page of each kind of copy shows what `cp` and `rsync`
   took beside rcp.

### Find the evidence of a run

Each run page names, under *Raw data*, where the worker's evidence is retained and until about when:
`production/runs/<run key>/collection/` in the benchmark bucket, for 90 days. Fetch and verify it
with operator credentials:

```bash
just aws-benchmark-evidence repo-655073755-run-43075583972 "$HOME/evidence-43075583972"
```

Give an absolute path outside the repository: `just` runs from the repository root, and this
evidence is not public. The directory must not exist yet and its parent must; it is created readable
by its owner only. It receives `manifest.json` and the files that manifest names (`report.json`,
`summary.html`, `controller.json`, `durable.json`, and `status.json` and `results.zip` when the
worker produced them), each read at its retained version and checked against the manifest's size and
SHA-256. The helper also prints the manifest's own digest, which is the `retirement_sha256` of the
run's public record. Start with `summary.html` and `report.json`: they hold every check and its
reason. The worker's own results and logs are in `results.zip` under `benchmark/results/`, with each
copy's output in `logs/<file set>/<scenario>/<copy>/`. The archive is never unpacked by the helper;
what a worker wrote is untrusted data, to be listed and read, not run.
