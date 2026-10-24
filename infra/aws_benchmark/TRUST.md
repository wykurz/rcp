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
permitted main workflows and repository administrators remain trusted. On compromise, audit those
surfaces as well as history; restoring history alone does not contain the incident.

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
  and only the environment secret named above.
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

With an already-authorized operator, capture repository metadata, public `/apps/{slug}` metadata,
environment detail, both ruleset detail objects, deployment branch policy and complete names-only
secret/variable inventories. Do not give either runtime App Administration permission for this
check. The repository capture must prove ID `655073755`, owner `483465`, `wykurz/rcp`, and
`fork: false`. App metadata must bind App ID, client ID, owner, exact permissions and empty events.
Ruleset detail must include `bypass_actors`; omission does not prove an empty list.

The repository and App captures use `{ "endpoint": "<exact API URL>", "response": {...} }`.
Environment and rulesets retain raw detail responses. Branch policies use an endpoint/response
envelope. The inventory format and independent GitHub-shaped fixtures are specified by
`production_trust.py` and the synthetic
[readback fixture](fixtures/history-trust-readbacks-v2.json); each paginated capture binds exact
endpoint, `per_page=100&page=N`, typed total counts, complete pages and unique names. Inventory
covers:

- Repository Actions and Dependabot secret names.
- The complete environment list and every environment's secret names.
- Repository variables and every environment's variable names/values, to verify scopes and gates.

Preserve these snapshots and their SHA256 fingerprint with the approval. They are operator-captured
observations, not remote attestations. No secret values are fetched. Matching the configured secret
name cannot detect a copied PEM under a different name or outside GitHub. The checker explicitly
leaves key custody, live enforcement and installation scope unproven.

```sh
python -m infra.aws_benchmark.production_trust writer-check \
  --app-id "$WRITER_APP_ID" --client-id "$WRITER_CLIENT_ID" \
  --installation-id "$WRITER_INSTALLATION_ID" \
  --repository-readback repository.json --app-readback app.json \
  --environment-readback environment.json --branches-readback branches.json \
  --rulesets-readback rulesets.json --inventory-readback inventory.json \
  --key-state absent --output writer-readback-before-import.json
```

Run the absent-key phase before storing the key, then capture fresh inventories and repeat with
`--key-state present` and a new output path after the separately approved import. Neither phase
changes readiness.

Separately inspect the actual installation's account, repository selection and permissions using an
authorized setup principal. Retain that evidence. The workflow's returned installation-ID check only
detects continuity/reinstallation; it does not detect broader permissions or repositories on an
unchanged installation. Token narrowing limits the requested token but does not constrain a stolen
signing key.

## Approval wait and publication

Publication rejects reruns. It exports a record count, skips the protected writer job for an empty
batch, and prepares a validated commit against fresh anonymous history **before** minting a token.
An already-published replay also needs no token. Semantic validation is tested with recomputed valid
inventories so a schema check cannot be accidentally replaced by a checksum-only test.

After environment approval, explicit checks validate identity, supplied flags and current main. The
exact main ref is rechecked immediately before mint and each push, so an approval for a superseded
workflow cannot publish. This is an observation followed by a network action, not an atomic lock on
main. The writer uses bounded non-force conflict reconciliation and never executes history files.

GitHub does not document that re-expanding a repository variable after a long approval wait fetches
its newest value. The repeated flag checks are defense in depth, **not a proven fresh kill switch**.
Closing publication/readiness/canary flags must also reject all pending environment approvals and
cancel affected waiting/running workflows; do not approve an old queued run afterward. A separately
approved timing canary must hold a job, close flags, and verify rejection/cancellation before
routine use. A fresh Variables API read would require additional permission, which this change does
not add.

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
ordinary token has Contents write solely to provide a meaningful positive control. It has no AWS or
OIDC permission. Enabling and dispatching it requires exact live approval; this PR does neither.

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

Every mutation/observation appends bounded public evidence. The receipt includes trust fingerprint,
reviewed SHA, control names, outcomes and tips; response hashes preserve linkage without logging raw
authenticated diagnostics. Evidence is uploaded on success or hold. Preserve the original snapshots,
GitHub run URL with the receipt for acceptance review. Raw rename bodies are not retained;
unfamiliar responses require separately approved diagnosis, not an automatic canary retry. No
automated result sets readiness true. Failures preserve state for reconciliation, not reruns.

## Explicit Secrets Manager reader boundary

The user must select an existing same-account recovery IAM role. No administrator, root, user or
session ARN is substituted. The setup CLI derives the reader roles from the exact captured disabled
stack's `AdmissionRoleArn` and `CollectorRoleArn` outputs; independently verify stack identity,
template provenance and role trust. Stack output text alone is not live effective-permission proof.

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
  --output reader-secret-policy-proposal.json
```

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

Rollback closes flags and rejects/cancels pending work first. Preserve history/rulesets and existing
benchmark shutdown guards. App/key revocation, secret/policy changes, debug retirement and any
cleanup after a failed canary need their exact approval. The old qualification stays shut down.

References: [rulesets](https://docs.github.com/en/rest/repos/rules),
[environments](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments),
[variables and timing](https://docs.github.com/en/actions/reference/workflows-and-actions/variables),
[branch rename](https://docs.github.com/en/rest/branches/branches#rename-a-branch),
[App metadata](https://docs.github.com/en/rest/apps/apps),
[pinned token action](https://github.com/actions/create-github-app-token/tree/bcd2ba49218906704ab6c1aa796996da409d3eb1),
[Secrets Manager resource policies](https://docs.aws.amazon.com/secretsmanager/latest/userguide/auth-and-access_resource-policies.html),
[replication](https://docs.aws.amazon.com/secretsmanager/latest/userguide/replicate-secrets.html),
[Lambda execution roles](https://docs.aws.amazon.com/lambda/latest/dg/lambda-intro-execution-role.html),
[PassRole](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_passrole.html).
