# Production credential and history trust setup

This is an offline setup proposal. It creates no App, key, secret, environment, ruleset, policy or
AWS resource. Publication and production remain disabled. The selected reader custody is **Secrets
Manager**; no KMS implementation is included. Merging these files is not live setup approval.

## Two separate credentials

| Consumer                     | Dedicated App permissions     | Key custody                                                                          | Installation      |
| ---------------------------- | ----------------------------- | ------------------------------------------------------------------------------------ | ----------------- |
| AWS admission and collection | Actions read, Metadata read   | AWS Secrets Manager `/rcp/production/github-read-app`                                | Only `wykurz/rcp` |
| Public history writer        | Contents write, Metadata read | GitHub environment `aws-benchmark-history`, secret `RCP_AWS_HISTORY_APP_PRIVATE_KEY` | Only `wykurz/rcp` |

Make both Apps private to owner `wykurz`, with no events/webhooks, organization/account permissions,
Actions write, workflow write, Administration, Pull requests or user OAuth flow. Verify the actual
App metadata and installation selection, not just typed IDs. The reader must never receive the
writer key. Key holders can administer their App's installations; runtime route restrictions do not
remove that capability. Select rotation/review owners before activation.

The writer action requests a short-lived token narrowed to `wykurz/rcp`, Contents write and Metadata
read. GitHub has no branch-scoped App token. This is **repository-wide authority**, including other
unruled branches, debug/history refs, release tags and mutable release assets. A tag-protected
release can still be affected by a branch shadowing its short tag name if a consumer does not
resolve explicit refs. Main rules do not protect all of these surfaces. Before activation, inventory
branch/tag rules, release asset mutability and consumers of ambiguous refs, and explicitly accept
the residual reach or approve separate protections. This change does not modify release workflows,
immutable-release settings, unrelated rules or main's solo-maintainer review policy. The key,
permitted main workflows and repository administrators remain trusted. On compromise, follow the
server-enforced freeze and owner suspension procedure below, then audit those surfaces as well as
history; restoring history alone does not contain the incident.

## Writer environment and reserved history namespace

Only the publication and explicit canary workflows currently reference `aws-benchmark-history` and
its key; tests check the complete workflow inventory. Any future trusted main workflow could request
this environment, so preserve main review/protection. The owner-only approval gate is the initial
policy; removing it requires a separate decision. Neither workflow is scheduled.

Configure and read back the environment **before storing a key or running either lane**. GitHub can
automatically create a missing environment without protection. Require:

- Deployment branch policy exactly `main`, type `branch`, with selected branches enabled. No tags,
  wildcards, debug branch or all-protected-branches shortcut.
- Required reviewer `wykurz` (ID `483465`), self-review allowed for this sole-owner model,
  administrator bypass disabled. The documented environment PUT payload does not set the UI bypass
  control. Read back `can_admins_bypass: false` after every environment write, including a
  renderer/tool reapply.
- Environment variables `RCP_AWS_HISTORY_APP_CLIENT_ID` and `RCP_AWS_HISTORY_APP_INSTALLATION_ID`,
  with the writer key stored only in the named environment secret. Unrelated secret names are
  allowed by this metadata check and need separate review.
- No custom deployment protection rules: both lanes use `deployment: false`, incompatible with
  custom rules. Capture the separate `deployment_protection_rules` endpoint with explicit zero count
  and an empty `custom_deployment_protection_rules` list; environment detail is insufficient.
- Repository variables `RCP_AWS_HISTORY_TRUST_READY`, `RCP_AWS_PRODUCTION_PUBLISH_ENABLED` and
  `RCP_AWS_HISTORY_CANARY_ENABLED`, all explicitly `false`. Do not place copies of these gate
  variables in environments. Environment variables are not available when job-level gates are
  evaluated.

Two active rulesets protect both `refs/heads/aws-benchmark-history` and its descendants matched by
`refs/heads/aws-benchmark-history/**/*`. The descendant reservation prevents a branch such as
`aws-benchmark-history/x` from occupying the namespace before bootstrap. The publisher still writes
only the exact root ref.

1. `rcp-aws-history-writer` restricts creation and updates, bypassed only by the exact writer **App
   ID**, not installation ID or the all-Actions App. For this verified non-fork repository, the
   API's bare `update` rule is the expected representation. No unrelated bypass actors are allowed.
2. `rcp-aws-history-preserve` prohibits deletion and non-fast-forward pushes with **no bypass
   actors**, including for the writer. Separate rules keep its update bypass from defeating
   preservation.

Before applying rules, inspect the root and all descendants. Any preexisting ref is untrusted
bootstrap state and a setup hold; do not declare it trusted because it looks useful or has an empty
tree. Do not delete or rewrite it automatically. Rules do not prevent a permitted writer from a
malicious fast-forward change or an administrator from editing policy. A checksum beside editable
records does not establish trust.

## Offline proposal and captured readback

Supply actual non-secret IDs, never private keys or tokens in CLI arguments. Use exclusive
`--output` files; errors go to stderr without clobbering an existing proposal. Shell redirection
truncates its file before validation and should not be used for these proposals.

```sh
python -m infra.aws_benchmark.production_trust writer-plan \
  --app-id "$WRITER_APP_ID" --client-id "$WRITER_CLIENT_ID" \
  --installation-id "$WRITER_INSTALLATION_ID" --output writer-plan.json
```

The plan separates repository variables from environment variables. It is not an executable omnibus
API payload. Obtain approval for exact resolved targets and baseline diffs, preserve unrelated
settings and stop on drift. Typed installation IDs are not installation-scope verification.

With an already-authorized operator, capture repository metadata, owner-authenticated `/apps/{slug}`
metadata, environment detail, both ruleset detail objects, deployment branch policy, custom
deployment rules, complete secret-name inventories and variable names/values. Do not give either
runtime App Administration permission for this check. The repository capture must prove ID
`655073755`, owner `483465`, `wykurz/rcp`, and `fork: false`. App metadata must bind App ID, client
ID, owner, exact permissions and empty events. Ruleset detail must include `bypass_actors`; omission
does not prove an empty list. As additional operator evidence, capture effective rules for
`rules/branches/aws-benchmark-history` and one named descendant and compare the reported ruleset IDs
with the approved pair. GH013 alone names violated rules, not their source ruleset. This optional
effective-rules capture is separate manual evidence; the local settings checker does not claim to
validate it.

The repository and App captures use `{ "endpoint": "<exact API URL>", "response": {...} }`.
Environment and rulesets retain raw detail responses. Branch policies use an endpoint/response
envelope. The inventory format and independent GitHub-shaped fixtures are specified by
`production_trust.py` and the synthetic
[readback fixture](fixtures/history-trust-readbacks-v2.json); each paginated capture binds exact
endpoint, typed total counts, complete pages and unique names. Use `per_page=30&page=N` for
repository/environment variables and `per_page=100&page=N` for secrets/environments. Inventory
covers:

- Repository Actions and Dependabot secret names.
- The complete environment list and every environment's secret names.
- Repository variables and every environment's variable names/values, to verify scopes and gates.

Preserve exact snapshot bytes and the capture manifest defined below with the approval. They are
operator-captured observations, not remote attestations. No secret values are fetched. Matching the
configured secret name cannot detect a copied PEM under a different name or outside GitHub. The
checker explicitly leaves key custody, live enforcement and installation scope unproven.

```sh
python -m infra.aws_benchmark.production_trust writer-check \
  --plan writer-plan.json --phase setup \
  --repository-readback repository.json --app-readback app.json \
  --environment-readback environment.json --branches-readback branches.json \
  --rulesets-readback rulesets.json --inventory-readback inventory.json \
  --custom-rules-readback custom-rules.json \
  --key-state absent --output writer-readback-before-import.json
```

Run the absent-key phase before storing the key, then capture fresh inventories and repeat with
`--key-state present` and a new output path after the separately approved import. Record the writer
key ID and public-key SHA256 fingerprint with its App/installation, import receipt, approved plan
and snapshot fingerprints; never record the PEM. Compare that fingerprint during rotation or
incident recovery. The checker binds the approved plan and records its canonical compact-JSON SHA256
digest; a configured installation ID remains an unverified scope claim. Neither check changes
readiness.

Use `--phase setup` with all flags false, `--phase canary` with only the canary flag true, or
`--phase publication` with publication/readiness true and canary false. Capture fresh readbacks
after each approved settings change; phase selection validates the expected state, not permission to
enable it. The same approved plan remains the identity/policy baseline.

`writer-check` snapshots each input once and emits `trust_manifest.capture_sha256`: SHA256 of exact
raw bytes for `plan`, `repository`, `app`, `environment`, `branches`, `rulesets`, `inventory` and
`custom_rules`. The manifest also contains schema `rcp-history-trust-captures-v1`, `phase` and
`key_state`. `trust_sha256` is SHA256 of that manifest serialized with Python
`json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)`
encoded as UTF-8 without a trailing newline. Paths and the containing output are excluded. This is
distinct from the canonical plan-only `approved_plan_sha256`.

For the canary, repeat the command on fresh retained captures using `--phase canary`,
`--key-state present` and `--output writer-readback-canary.json`. Read `trust_sha256` with
`jq -er .trust_sha256 writer-readback-canary.json` and supply that exact approved workflow input.
During acceptance, rerun writer-check on the same retained bytes into a new output; compare its
manifest/digest with the approval and every canary receipt. Even formatting changes alter the
capture digest. The canary records the approved value, rather than independently reading settings.
Manual key/visibility/installation evidence remains separate from this API-capture manifest.

App visibility and the registered key set require owner-UI evidence. API metadata cannot prove
private visibility, and anonymous 404 is also compatible with a wrong slug. At each key-state check,
before the canary and before readiness, retain the Credentials / Key pairs inventory. Require one
registered key matching the approved public fingerprint. An absent environment secret does not mean
the App has no keys. Before approved disposal of the securely downloaded PEM, compare its public
fingerprint with GitHub's entry:

```sh
openssl rsa -in PATH_TO_PEM_FILE -pubout -outform DER | openssl sha256 -binary | openssl base64
```

This separately approved key-handling procedure is not run by the PR. Retain only the public result,
never PEM bytes in evidence or arguments. Additional keys or an unknown fingerprint hold acceptance;
registered keys do not expire automatically.

Separately inspect the actual installation's account, repository selection and permissions using an
authorized setup principal. Retain that evidence. The workflow's returned installation-ID check only
detects continuity/reinstallation; it does not detect broader permissions or repositories on an
unchanged installation. Token narrowing limits the requested token but does not constrain a stolen
signing key.

## Approval wait and publication

Publication rejects reruns. It exports a record count, skips the protected writer job for an empty
batch, and prepares a validated commit against fresh anonymous history **before** minting a token.
Every publication phase requires the established canary-created root; absence holds for
reconciliation and never silently starts a new lineage. An already-published replay also needs no
token, though a replay-only batch still requests owner approval before preparation discovers the
no-op. An empty batch skips publication and Pages. To recover a failed or stale Pages deployment
without new records, dispatch `benchmark-pages.yml` directly on current main under its existing
gates; do not rerun an old publication. Semantic validation is tested with recomputed valid
inventories so a schema check cannot be accidentally replaced by a checksum-only test.

After environment approval, explicit checks validate identity, supplied flags and current main. The
exact main ref is rechecked immediately before mint and each push, so an approval for a superseded
workflow cannot publish. This is an observation followed by a network action, not an atomic lock on
main. The writer prepares one commit against one exact history baseline before minting. Every push
binds that baseline with an exact expected-tip lease; construction verifies the commit is its
fast-forward child. A lost response may reconcile only to that exact prepared commit or the
unchanged baseline. Any foreign tip, including a fast-forward, holds for a fresh credential-free
preparation rather than being adopted during token-bearing retries. History files are never
executed.

Publication keeps anonymous Git reads. GitHub may temporarily require authenticated HTTPS reads
while permitting pushes. An unavailable read holds publication; preserve any uncertain push evidence
and dispatch a fresh approved run after reads recover. This limitation does not authorize minting a
credential to bypass a read failure. Errors distinguish an unavailable main read from changed main.
Canary observations instead use the ordinary credential already held for its controls.

GitHub does not document that re-expanding a repository variable after a long approval wait fetches
its newest value. The repeated flag checks are defense in depth, **not a proven fresh kill switch**.
The server-enforced history freeze below is the write cutoff. Closing publication/readiness/canary
flags must also reject all pending environment approvals and cancel affected waiting/running
workflows; do not approve an old queued run afterward. A separately approved timing canary must hold
a job, close flags, and verify rejection/cancellation before routine use. A fresh Variables API read
would require additional permission, which this change does not add.

Public and raw handoff artifacts expire after one day. Approve promptly within that retention
window; if an approval is delayed or artifacts expire, reject the waiting run and dispatch a fresh
one. A 30-day environment waiting limit does not preserve a one-day artifact. Main movement likewise
requires a fresh run. Missing artifacts or a stale revision stop before minting.

Only the mint step references the writer key. The official pinned action revokes its installation
token on normal job cleanup. Abrupt runner loss can leave it valid until its one-hour expiry. There
is no cross-job token handoff, token artifact or persistent credential helper. Never enable shell
tracing or diagnostic header/key/token logging.

## Separate writer acceptance lane

`aws-benchmark-history-canary.yml` is a separate, initially disabled, owner-only manual workflow on
current main, attempt 1. It uses the same protected environment and concurrency group as
publication, but requires canary enabled and both publication/readiness explicitly **false**. Its
ordinary token has Contents write for positive controls and authenticated Git observations. It has
Actions read to bind an explicitly approved failed-run artifact for recovery, and no AWS or OIDC
permission. This is a repository Actions read permission, not a per-artifact credential; it does not
grant Actions write or workflow dispatch. Enabling and dispatching it requires exact live approval;
this PR does neither.

The approval must name the reviewed main SHA, captured trust fingerprint, actual Apps/installation,
and unique 16-hex `probe_id`. This determines exactly two disposable positive-control refs:
`refs/heads/rcp-history-canary-<probe_id>-source` and
`refs/heads/rcp-history-canary-<probe_id>-renamed`. Approve their creation, rename and
successful-path cleanup, plus the root history bootstrap and negative operations. Unexpected allowed
operations may change history: record that possibility explicitly. The script never resets or
repairs history.

The executable lane:

1. Requires root, descendants and both control refs absent. Creates and renames a control with the
   ordinary credential to prove real authority, including settled rename readback. Each rename
   freshly checks its source tip and absent destination, but the REST API has no expected-tip/CAS
   parameter. That observation is not an atomic guarantee against a concurrent writer changing the
   disposable source. Keep other writers quiescent and account for that race in the live approval;
   observed drift stops the test. Git pushes separately use exact expected-tip leases.
2. Tests ordinary root creation and rename into the history namespace **before** minting the writer
   token. Generic permission errors, 404s, 403s/422s, auth failures and transport failures are
   inconclusive. A rename rejection needs policy-specific evidence; GitHub does not document a
   stable ruleset-specific rename error schema, so an unfamiliar live response holds acceptance for
   review.
3. Uses the writer to bootstrap and fast-forward empty-tree sentinel commits, then tests ordinary
   fast-forward rejection and writer/ordinary deletion and non-fast-forward rejection. The latter
   sends a real unrelated root with an exact expected-tip lease; client-side non-FF failure is not
   proof. Push denials require exact-ref server rejection, GH013 and unchanged readback.
4. Fetches depth 1 and calls the real publisher history reader, requiring zero records and the exact
   tip. All sentinels use Git's empty tree; no test file can poison strict public history. Removes
   only the named positive-control ref after success. Retains the empty history commits.

Every mutation/observation appends bounded public evidence, including precondition holds and rename
polls. The receipt includes trust fingerprint, reviewed SHA, control names, outcomes and tips, plus
bounded sanitized Git rule/porcelain and rename message/error diagnostics with response hashes.
Authenticated headers and tokens are excluded. Evidence is uploaded on success or hold with 90-day
retention; preserve it, the original snapshots and the GitHub run URL for acceptance review. Known
exact-source/destination policy-denial shapes are supported; an unfamiliar response still holds. No
automated result sets readiness true.

A separately approved fresh `mode: resume` dispatch can finish after a definitive writer mutation
and a later read failure. Approval must bind the original failed/cancelled/timed-out run, exact
artifact ID, receipt SHA256, probe ID, trust fingerprint and unchanged reviewed main SHA. The
workflow reads and binds original run/artifact metadata before downloading; it rejects expired,
wrong-run, wrong-head or non-attempt-1 evidence before minting. The new receipt preserves the
approved original bytes and records the recovery binding. A locally editable receipt is not an
independent remote attestation; the owner must inspect its source and digest before approving
recovery.

Recovery reconstructs the deterministic empty-tree sentinels before minting, verifies a contiguous
sequence of definitive mutation outcomes, rechecks the exact current refs and skips only proven
completed mutations. Bounded idempotent reads may retry; mutations are single-shot. An intent
without a definitive outcome, unexpected allow, foreign tip, changed main or incomplete ordinary
controls holds for separate reconciliation. This does not permit rerunning the old workflow or
regenerating a receipt to declare uncertain work complete. No history rewrite, deletion or policy
relaxation is part of recovery.

## Explicit Secrets Manager reader boundary

The user must select an existing same-account recovery IAM role outside the production stack. No
production service/job role, administrator, root, user or session ARN is substituted. The setup CLI
derives the reader roles from the exact captured disabled stack's `AdmissionRoleArn` and
`CollectorRoleArn` outputs; independently verify stack identity, template provenance and role trust.
The checker excludes recovery identities present in stack outputs; the operator must also verify
that the selected recovery role is not another production service/job role absent from those
outputs. Stack output text alone is not live effective-permission proof.

The stack capture is
`{ "operation": "DescribeStacks", "region": "<region>", "parameters":
{ "StackName": "<exact stack ARN>" }, "response": <raw DescribeStacks JSON> }`.
Capture the exact request, complete terminal response, disabled parameters and role outputs; never
substitute another stack's outputs with the same display names.

Use this order to avoid a readable-key bootstrap interval:

1. Separately approve/deploy the disabled stack with empty App parameters; reader roles exist but
   App-secret read grants are absent.
2. Create an **empty** external secret with no PEM. Capture exact ARN and disabled stack outputs.
3. Render, approve, attach and read back the deny policy while the secret is empty. Preserve any
   existing policy and compose the reviewed diff; `PutResourcePolicy` replaces it. Use
   `BlockPublicPolicy`.
4. Only after policy verification, separately approve secure PEM import and disabled App-parameter
   configuration, followed by live reader-role tests. Never use shell/CLI arguments for the PEM.

```sh
python -m infra.aws_benchmark.production_trust reader-policy \
  --secret-arn "$READER_SECRET_ARN" --stack-id "$PRODUCTION_STACK_ID" \
  --stack-readback disabled-stack.json --recovery-role "$RECOVERY_ROLE_ARN" \
  --phase bootstrap \
  --output reader-secret-policy-proposal.json
```

The default `--phase bootstrap` requires all App parameters empty. A later separately approved
`--phase rebind` requires complete App parameters and an exact match between the configured secret
suffix and attachment ARN, with the same disabled runtime/admission/publication controls. Neither
phase reads secret content or proves whether it is empty; retain the empty-secret creation receipt
and verified policy readback before import.

The output is an envelope containing `attachment` (exact secret ID and BlockPublicPolicy),
`resource_policy`, exact stack/reader/recovery identities and a live acceptance requirement. Do not
submit the envelope as the policy document. The attached policy grants nobody access. It denies
GetSecretValue outside the two exact reader roles and the selected recovery role; reader roles are
also denied absent/non-AWSCURRENT stage context. The identity policies independently require
explicit AWSCURRENT. The deny statements use `Resource: "*"` **within the policy attached to this
exact secret**, so replicated policies still cover replica ARNs. This is not a global identity
policy or an allow. Replication is not authorized; AWS documents that resource policies replicate
while the region part of the ARN changes. Review any existing replica inventory and test the actual
attachment/conditions before claiming enforcement.

Recovery needs separately reviewed identity permission/trust and is not stage-limited by this deny,
so approved recovery can inspect old versions. It gains no implicit rotation/deletion authority.
Omitted-stage, VersionId-only, AWSPREVIOUS and unrelated-principal negatives need real positive
controls. Missing secrets/versions are not IAM evidence.

Role replacement or stack recreation is a hold: capture new outputs, remove obsolete exemptions and
approve/read back the updated resource policy before enabling new roles. Do not leave retained old
collector roles exempt merely because their historical ARN once appeared in a packet.

Secret/resource-policy administrators remain trusted. So do principals able to update reader Lambda
code/configuration/layers, create code with PassRole, deploy its stack, change reader-role trust or
otherwise execute as an exempt role. Inventory and restrict those control-plane paths in the exact
live packet. A resource deny cannot distinguish legitimate reader code from injected code running as
the same role. A copied PEM and already minted tokens survive a policy change; contain access and
outstanding jobs before separately approved key rotation. No live control-plane restrictions are
silently added by this template preparation.

## Approval and acceptance sequence

1. Resolve and approve Apps/installations, custody/recovery identity, exact environment/rulesets,
   empty-secret policy sequence and disabled deployment artifacts. Preserve unrelated resources.
2. Pin the compatible AMI, package/source and numeric template; approve any necessary paid build
   separately. The template's service roles and live grants remain deployment approvals.
3. Perform the exact reader-role, S3/DynamoDB and route-bound probes described in
   [PRODUCTION.md](PRODUCTION.md). Those probes are not an implemented OIDC canary: separately
   verify real GitHub OIDC claim shapes, role assumption positives/negatives and branch/environment
   rejection.
4. Approve the bounded writer lane and approval-delay test above, with flags otherwise off. Review
   all evidence and holds, including installation scope and broader Contents-write reach.
5. After live checks and production qualification, separately approve readiness/publication. Verify
   rendering, persistence and cleanup, then decide cadence/monthly allowance and debug retirement.

## Server-enforced freeze and compromise response

These are proposed incident/rollback actions requiring exact live approval, not actions performed by
this PR. Obtain approval for the current rule ID, App/installation IDs and baseline diff; preserve
unrelated settings and stop on drift.

1. Remove only the writer App from `rcp-aws-history-writer`'s bypass actors, leaving the active
   creation/update restrictions and `rcp-aws-history-preserve` unchanged. Read back the exact rule.
   GitHub applies these restrictions at each write, including pushes from already-minted tokens; no
   bypass actor means no permitted history writer. This freezes only the reserved history namespace,
   not the key's repository-wide authority. Ruleset administrators remain trusted.
2. For suspected key/token compromise, have the installation owner suspend the exact writer
   installation through GitHub's owner UI. Prefer owner suspension to App-manager/API suspension:
   the owner suspension cannot be undone with the copied App signing key. Preserve evidence and
   review exact key revocation separately; do not assume rotating a key invalidates minted tokens.
   GitHub requires a replacement before deleting the only registered key. Under exact incident
   approval, keep the installation suspended, generate/import one replacement through trusted
   custody, delete all prior keys and verify the Key pairs list contains only the replacement's
   recorded fingerprint. Each creation/import/deletion needs its approved scope. Deleting the App
   registration is a separate destructive alternative; rotation is never automatic.
3. Close publication/readiness/canary flags, reject pending environment approvals and cancel
   affected workflows as defense in depth. Preserve history, the preservation ruleset and existing
   benchmark shutdown guards. Preserving rulesets permits the tightening bypass removal above.
4. Review the affected key fingerprint, installation and broader Contents-write surfaces before
   recovery. Re-adding the exact App bypass, owner unsuspension, key replacement, any failed-canary
   cleanup, debug retirement and secret/policy changes each require their exact approval and fresh
   readback/live acceptance. Never restore access automatically from a workflow result.

The old qualification stays shut down.

References: [rulesets](https://docs.github.com/en/rest/repos/rules),
[creation/update restrictions](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets),
[installation suspension API](https://docs.github.com/en/rest/apps/apps#suspend-an-app-installation),
[owner suspension boundary](https://docs.github.com/en/apps/maintaining-github-apps/suspending-a-github-app-installation),
[environments](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments),
[variables and timing](https://docs.github.com/en/actions/reference/workflows-and-actions/variables),
[branch rename](https://docs.github.com/en/rest/branches/branches#rename-a-branch),
[App metadata](https://docs.github.com/en/rest/apps/apps),
[private-key management](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/managing-private-keys-for-github-apps),
[custom protection rules](https://docs.github.com/en/rest/deployments/protection-rules#get-all-deployment-protection-rules-for-an-environment),
[temporary anonymous Git restrictions](https://github.blog/changelog/2026-10-19-some-git-over-https-may-temporarily-require-authentication/),
[pinned token action](https://github.com/actions/create-github-app-token/tree/bcd2ba49218906704ab6c1aa796996da409d3eb1),
[Secrets Manager resource policies](https://docs.aws.amazon.com/secretsmanager/latest/userguide/auth-and-access_resource-policies.html),
[replication](https://docs.aws.amazon.com/secretsmanager/latest/userguide/replicate-secrets.html),
[Lambda execution roles](https://docs.aws.amazon.com/lambda/latest/dg/lambda-intro-execution-role.html),
[PassRole](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_passrole.html).
