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
Already-published history is never rewritten. Public v1 success/failure records and v2's distinct
evidence-unavailable outcome remain readable. New v3 records carry a canonical collector-recorded
`collected_at`; old records display no trusted collection time instead of presenting the
candidate-controlled harness timestamp as trusted. A bounded diagnostic report can describe a failed
archive while preserving its exact raw bytes. The local publication helpers discover only retired
manifests, bind each to a consistent closed tombstone, and download exact retained versions through
read-only clients. Discovery traverses the full retained prefix within page/count/time bounds; it is
not an atomic snapshot, and later scans must revisit the prefix. A separate consistent
active-pointer observation describes pending ownership before a manifest exists, without claiming
physical teardown or a cross-service snapshot.

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

### Publication trust gate

Publication remains disabled pending the separate history-writer decision and live acceptance (F55).
A schema-valid Git record and a retirement digest are not independent authentication of its
measurement bytes. Another repository Contents-write credential can alter an unprotected history
branch, including through a fast-forward commit. Do not enable publication or AWS history rendering
by treating `RCP_AWS_HISTORY_TRUST_READY=true` alone as evidence of a protected branch. That
repository variable defaults off and gates both publication and AWS history rendering, independently
of `RCP_AWS_PRODUCTION_PUBLISH_ENABLED`. Publication and Pages reject reruns; submit a fresh manual
workflow after a hold instead of executing an old revision's write path.

The proposed setup requires separate approval before implementation or live configuration:

- Create an owner-managed, repository-selected App named `rcp-benchmark-history-writer`, installed
  only on `wykurz/rcp`, with Contents write and Metadata read. It must be separate from the
  Actions-read proof App. Approve its key custody, secret location and short-lived token minting
  workflow before generating or importing a key.
- Add an active branch ruleset matching exactly `refs/heads/aws-benchmark-history`: restrict
  creation, updates and deletion, and block force pushes. Give only that exact writer App the
  required bypass. Do not add an all-Actions, collaborator, role-wide or unrelated-App bypass.
  Preserve main protections; the App token itself is repository-scoped, not branch-scoped.
- Replace the writer's repository-wide job token with the reviewed dedicated token path, keep the
  exact refspec and credential-free export boundary, and verify denial with an ordinary workflow
  token and success only through the dedicated writer. Protect the token-minting job/configuration
  so arbitrary repository code cannot obtain the App key. Record resolved App/installation IDs,
  ruleset ID and evidence before enabling the readiness and publication gates.

This PR does not create that App, secret, ruleset or bypass and does not claim the current job token
satisfies this gate. Removing persisted checkout credentials from release jobs reduces exposure but
is not a replacement for history protection. Anchoring public digests in DynamoDB is a possible
additional integrity layer; it would still require a trusted verification path for standalone Pages
and does not replace the publication authorization decision.

Public pages distinguish consistency checks from measurement honesty. Candidate binaries share the
harness UID and can forge measurement files; the displayed values are self-reported, unattested
performance measurements. Verified cleanup describes independent teardown evidence only. Metadata
validation does not establish content integrity. Do not turn these labels into an accuracy or
performance claim without reviewing the candidate and measurement methodology.

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
watchdog, publication and GitHub job roles receive no secret access; existing account administrators
retain their existing powers.

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
2. Approve and review the one-time secure private-key import procedure, generate a key and store
   only its PEM in the exact external secret with AWS-managed encryption. Never paste it into chat
   or command arguments, put it in CloudFormation parameters, or commit it. The application requires
   no App OAuth client secret. Record the full secret ARN, its six-character suffix and public key
   fingerprint, never the key.
3. Prepare an account-resolved disabled change set using the client ID, installation ID, secret
   suffix and immutable package pins. The production stack is still undeployed: review the full
   nine-role/resource inventory and trust, including the two exact secret-read policy resources.
   Those App additions do not replace approval of the wider deployment. Preview generated role ARNs
   symbolically, then verify their actual values and resolved policies after approved disabled
   creation and before any same-role canary. App setup, secret creation/import and live IAM
   deployment each remain outside code-preparation authority.
4. Approve the exact **no-worker** probe mechanism below after the disabled deployment and App
   configuration review. Production handlers cannot establish this evidence with feature flags off
   and reserved concurrency zero. Nor does enabling the normal handler with a disabled server policy
   help: the early policy gate rejects before authenticated GitHub reads. Keep those gates intact;
   do not dispatch a benchmark or enable timers as an authentication test. **F13 remains open until
   the separately approved live acceptance gate passes**.

### No-worker App and IAM acceptance

First review the resolved identity/resource policies and any secret resource policy. IAM simulation
can check expected AWSCURRENT, omitted-stage, VersionId-only, AWSPREVIOUS, other-secret and
unrelated Secrets Manager/KMS outcomes. Record all supplied context and unsupported policy types;
simulation is not proof of live service authorization and cannot substitute for exercising each
actual reader role. The `SimulatePrincipalPolicy` API documents resource-policy evaluation for IAM
users; do not present a supplied secret resource policy as proof of effective reader-role
authorization. An operator's own successful secret read is not evidence for either Lambda role.

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
2. Contain AWS access **before storing a replacement key**. Review the current secret resource
   policy and apply a separately approved explicit `Deny` for `secretsmanager:GetSecretValue` to the
   affected reader role ARNs, or a reviewed deny-except recovery-principal boundary if incident
   scope requires it. Preserve unrelated statements, use `BlockPublicPolicy`, and verify the
   effective denial with approved same-role evidence. Closing admission or zeroing Lambda
   concurrency alone leaves the secret grants in place. Do not claim account-wide exclusivity from
   the template's two allows.
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
does not provision or approve that alternative, a secret resource policy or a recovery role.

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
   including cancelled/failed producer recovery. Enable public reporting separately after approving
   its exact read-only role, enabling the separate `PublicationEnabled` gate, and setting
   `RCP_AWS_PRODUCTION_PUBLISH_ROLE`, `RCP_AWS_PRODUCTION_TABLE` and
   `RCP_AWS_PRODUCTION_PUBLISH_ENABLED`; it shares the existing production region/bucket variables.
   Keep `RCP_AWS_HISTORY_TRUST_READY` false until the separate history-writer/ruleset implementation
   and live positive/negative trust canary above are approved and verified. Only then enable that
   readiness flag for both publication and AWS history rendering. Pages retains its independent
   `RCP_BENCHMARK_PAGES` flag. Publication has no recurring trigger in this change. Broader
   comparisons and a few-times-weekly schedule require their reviewed contracts, operating allowance
   and activation decisions. Do not call this initial acceptance foundation a complete production
   cutover.

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
