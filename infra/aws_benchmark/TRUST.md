# Production credential and history trust setup

This is an offline setup proposal. It does not create an App, key, secret, environment, ruleset,
policy or AWS resource. Publication and production remain disabled. The selected AWS reader custody
is **Secrets Manager**, with the explicit read boundary below. No KMS implementation is included.

## Two separate credentials

| Consumer                     | Dedicated App permissions     | Key custody                                                                                  | Allowed installation |
| ---------------------------- | ----------------------------- | -------------------------------------------------------------------------------------------- | -------------------- |
| AWS admission and collection | Actions read, Metadata read   | AWS Secrets Manager `/rcp/production/github-read-app`                                        | Only `wykurz/rcp`    |
| Public history writer        | Contents write, Metadata read | The new GitHub environment `aws-benchmark-history`, secret `RCP_AWS_HISTORY_APP_PRIVATE_KEY` | Only `wykurz/rcp`    |

The reader must never receive the writer key. Neither App needs Actions write, workflow write,
Administration, Pull requests, organization/account permissions, webhooks or a user OAuth flow. Make
both Apps private to the owner, with no active webhook/events, and verify the actual installation
selection and permission inventory. The reader acceptance requirements in
[PRODUCTION.md](PRODUCTION.md) still apply. Key holders also possess the App's own
installation-administration capability; the code's route restrictions do not remove that authority.
Select key-rotation/review owners before activation.

The writer token is explicitly narrowed to this repository, Contents write and Metadata read. Its
App must not be installed on other repositories. GitHub does not provide a branch-scoped App token:
main protection, the exact history rulesets and reviewed main workflows remain trusted boundaries.
The default workflow token is now Contents read throughout publication. The writer has no AWS/OIDC
credential, and the exporter has neither the App key nor a write token.

## Writer environment and exact history rulesets

The writer job alone references `aws-benchmark-history`. Before any secret is stored or publication
flag enabled, separately approve and configure:

- Selected deployment branches, exactly `main` with type `branch`; not all protected branches, tags,
  wildcard patterns or the old debug branch.
- Required reviewer: owner `wykurz` (ID `483465`). Permit self-review because the sole manual
  requester and reviewer are the same owner. Disable administrator bypass in the UI and verify
  `can_admins_bypass: false` in the environment readback. The documented environment PUT schema does
  not provide that UI control, so the generated settings payload does not pretend to apply it.
- Environment-scoped client ID, installation ID and private-key secret. Do not create repository- or
  organization-level copies of the same secret. The two readiness/publication flags stay false.

Any trusted main workflow can reference this environment and request its secret; an environment is
not a workflow-path restriction. Review all main workflow changes and preserve main protections. The
owner approval gate is the proposed initial policy. Removing it for routine automation needs a
separate policy decision; this change does not enable a schedule or unattended publication.

Two active branch rulesets must match exactly `refs/heads/aws-benchmark-history`:

1. `rcp-aws-history-writer`: restrict creation and updates, including disabling fetch-and-merge
   updates. Only the exact writer App is a bypass actor. The bypass uses the numeric **App ID**, not
   its installation ID. No all-Actions, repository-role, collaborator or unrelated-App bypass.
2. `rcp-aws-history-preserve`: prohibit deletion and non-fast-forward pushes, with **no bypass
   actors**. Keep this separate so the writer's update bypass cannot also bypass preservation.

Rulesets protect Git operations, not the honesty of performance measurements or policy changes by
repository administrators. They do not stop an authorized writer from a malicious fast-forward
change. The reviewed writer additionally rejects conflicting records and never checks out history as
executable code. Repository administrators, the writer key and permitted main workflows remain
trusted. A checksum stored beside an editable record does not replace this trust.

After obtaining the actual non-secret IDs, render a proposal locally:

```sh
python -m infra.aws_benchmark.production_trust writer-plan \
  --app-id "$WRITER_APP_ID" --client-id "$WRITER_CLIENT_ID" \
  --installation-id "$WRITER_INSTALLATION_ID" > writer-plan.json
```

These values are intentionally not guessed or committed. The renderer refuses missing identities; it
checks syntax but does not verify that an App exists. It returns supported environment settings, the
exact branch policy, two ruleset payloads and still-disabled flag values. Do not apply this whole
JSON object to an API. Obtain approval for the resolved individual targets and diff first. Check for
existing names/settings, preserve unrelated rules and stop on baseline drift rather than
overwriting.

Capture the environment GET, both ruleset detail GETs and complete deployment-branch-policy response
with an authorized operator. Ruleset detail must include `bypass_actors`; GitHub can omit that field
when the reader lacks sufficient ruleset access. Missing does not mean empty. Do not add
Administration permission to either runtime App merely to perform this setup readback.

`writer-check` takes raw environment detail JSON, an array of the two ruleset detail JSON objects,
and this branch-policy capture envelope (the endpoint records the exact request made):

```json
{
  "endpoint": "https://api.github.com/repos/wykurz/rcp/environments/aws-benchmark-history/deployment-branch-policies",
  "response": {"total_count": 1, "branch_policies": [{"id": 123, "name": "main", "type": "branch"}]}
}
```

The ID above is a synthetic format example, not a selected live policy. Preserve the actual
response. An authenticated operator must collect these snapshots; locally editable JSON is not
remote attestation. The checker binds the environment URL and repository ruleset sources, requires
exact controls and fails on missing fields. It reports `live_enforcement_proven: false` even when
settings match. For example:

```sh
python -m infra.aws_benchmark.production_trust writer-check \
  --app-id "$WRITER_APP_ID" --client-id "$WRITER_CLIENT_ID" \
  --installation-id "$WRITER_INSTALLATION_ID" \
  --environment-readback environment.json --branches-readback branches.json \
  --rulesets-readback rulesets.json
```

The workflow validates the current-run public artifact before minting a token, checks the returned
installation ID before using it, and validates the same public batch again before creating a commit.
The pinned official token action revokes its token in its post-job cleanup. That cleanup is best
effort: abrupt runner loss may leave the token valid until its one-hour expiration. There is no
persistent Git credential helper, token artifact or cross-job token handoff. The key is referenced
only by the mint step; do not enable shell tracing or diagnostic header/token logging.

## Explicit Secrets Manager reader boundary

The external reader secret and its roles do not exist as resolved production resources yet. The user
must select an existing same-account **recovery IAM role**; no administrator, user, root or session
ARN is silently substituted. Obtain the actual AdmissionRole and CollectorRole ARNs from the
separately approved disabled stack and verify their identities and trust before rendering:

```sh
python -m infra.aws_benchmark.production_trust reader-policy \
  --secret-arn "$READER_SECRET_ARN" --admission-role "$ADMISSION_ROLE_ARN" \
  --collector-role "$COLLECTOR_ROLE_ARN" --recovery-role "$RECOVERY_ROLE_ARN" \
  > reader-secret-policy-proposal.json
```

The proposal contains only explicit denies on `GetSecretValue` for the exact secret:

- Deny principals other than the two reader roles and the explicitly selected recovery role, using
  exact `aws:PrincipalArn` IAM role ARNs. This includes other identity-policy allows and per-secret
  reads through `BatchGetSecretValue`.
- Deny the reader roles when the `secretsmanager:VersionStage` context is absent or differs from
  `AWSCURRENT`. The existing identity policies independently require explicit AWSCURRENT access.

The proposal grants nobody access. Recovery still needs its separately reviewed identity permission
and trusted assumption path. It deliberately does not constrain the recovery role to AWSCURRENT, so
approved incident recovery can inspect prior versions; that is not standing rotation/deletion
permission. Condition behavior and version-selection negatives require live verification: offline
predicate tests cannot establish what context AWS supplies for an omitted API parameter.

Read and preserve any existing resource policy before proposing the final composition. AWS
`PutResourcePolicy` replaces it: never blindly overwrite unrelated statements with this fragment.
Use `BlockPublicPolicy`, review the exact diff and verify effective allowed/denied calls under the
actual reader/recovery roles. Secret/resource-policy administrators remain trusted and can change
this boundary. A deny does not erase a previously copied PEM or revoke already minted GitHub tokens.
No PEM, AWS keys or access tokens belong in CLI arguments, logs, source, review comments or chat.

## Approval and acceptance sequence

1. Approve exact owner-only App registrations/installations, key custody/import, resolved recovery
   identity and secret-policy composition. Writer credentials live only in the new environment; the
   reader key lives only in the approved AWS secret. Preserve existing unrelated credentials.
2. Approve exact environment/ruleset changes, disabled production stack and versioned code/template
   uploads. Leave all production/publication flags false. The merged template has nine service/job
   roles; no grant is implied by this renderer. Resolve production profile/bucket, rebuild the
   compatible worker image and pin the final package/source/numeric launch template separately.
3. Perform the separately approved reader-role, S3/DynamoDB and OIDC canaries in PRODUCTION.md.
   Verify the secret boundary with real positive controls, not missing-secret/version errors.
4. Before real history exists, approve a bounded writer canary using benign sentinel commits with
   identical harmless public data. Verify owner approval/main-only environment selection, the exact
   App installation/token permissions, ordinary-token update denial, allowed App fast-forward
   updates and App/ordinary-token deletion/non-fast-forward denial. The ordinary credential must
   have valid Contents-write authority without a ruleset bypass; the default read-only workflow
   token is not a meaningful negative control. Approve a harmless positive-control ref separately to
   establish that write authority. Expired credentials, malformed requests and not-found responses
   are inconclusive. Approve the exact sentinel IDs and possible unexpected effects first. Any
   unexpected allowed operation holds activation; do not reset/delete history or alter rules to
   finish the test. Missing or already valuable history requires a newly reviewed canary plan rather
   than a destructive experiment.
5. After the live checks and one production acceptance run, separately approve enabling
   `RCP_AWS_HISTORY_TRUST_READY` and publication. Verify rendering/history persistence and token
   cleanup, then decide cadence, ongoing allowance and eventual retirement of exact debug access.

Rollback closes publication/readiness gates first and preserves history/rulesets. Revoke or rotate
an App key, remove a secret, modify a resource policy, retire debug access or delete canary
resources only under its exact approval. Do not automatically remove preservation rules or disable
benchmark shutdown guards. The old qualification remains shut down; no recovery session is needed
for this local preparation.

References: [GitHub rulesets](https://docs.github.com/en/rest/repos/rules),
[environment settings](https://docs.github.com/en/rest/deployments/environments),
[branch policies](https://docs.github.com/en/rest/deployments/branch-policies),
[pinned token action](https://github.com/actions/create-github-app-token/tree/bcd2ba49218906704ab6c1aa796996da409d3eb1),
[Secrets Manager resource policies](https://docs.aws.amazon.com/secretsmanager/latest/userguide/auth-and-access_resource-policies.html),
[principal ARN condition](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-principalarn).
