# Publishing credential boundary

This is a code and setup proposal. Merging it does not create an environment, move or revoke a
credential, approve a release, or change AWS permissions. Complete the separately approved cutover
below before provisioning the benchmark history writer App/key. Until then, the repository-level
`CRATES_IO_TOKEN` remains accessible to historical workflows, even though current code no longer
references it.

## Workflow behavior

Only `release.yml`'s `publish-crates` job and `publish.yml`'s actual `publish` job request the
`crates-io` environment and `CRATES_IO_PUBLISH_TOKEN`. They check that the credential is present
before running selected-source shell commands. Their environment URL identifies the resolved commit
for the approver. Keep the secret only in that environment: a repository or organization secret with
the same name would defeat the intended isolation because the `secrets` context does not attest
where a value came from.

The standalone `publish.yml` dry run is a separate job with no environment, publishing-secret
reference or OIDC permission. It still validates the held source and runs the fixed `--dry-run`
command. It has the ordinary read-only workflow token; "credential-free" here means no publishing
credential. `release.yml` dry runs continue to skip publishing jobs and release writes. Neither dry
run is evidence that a real publishing token is authorized for every workspace crate.
`cargo-workspaces` 0.4.2 can skip versions already present in the index and report per-crate
packaging errors as warnings. A successful dry-run command is not proof that every crate packaged
successfully. The source validation jobs remain required.

Both caller workflows set `cache-mode: none`, which limits the runner's scoped cache token and
propagates into reusable validation. Selected release source cannot restore or save shared Actions
caches, even during a dry run dispatched on `main`. Read-only `GITHUB_TOKEN` permissions or simply
turning off a cache action would not enforce that boundary. This may make release validation slower;
ordinary CI keeps its existing cache policy. Offline tests assert the caller limit and reject
conflicting job/callee overrides; they do not emulate GitHub's cache service. Historical controllers
retain their original cache access until retired.

Both publishing paths preserve the [resolved release identity](release-identity.md). A modern
controller dispatched on `main` can select an older tag, provided its source supports the required
build helpers. No new helper script is required in that older source by this credential change.
Tag-push release controllers are preserved. Rerunning an old workflow does not upgrade it to current
code; recovery should dispatch the reviewed controller instead.

## Proposed live configuration — separate approval required

Repository: `wykurz/rcp`. Environment: `crates-io`.

- Enable selected deployment branches/tags: exactly branch `main` and tag `v*`, using separate typed
  policies. Do not substitute protected-branches-only or permit a branch named `v*`.
- Require reviewer `wykurz` (ID `483465`), allow self-review, and disable administrator deployment
  bypass. This is explicit sole-owner approval, not two-person control. Another available reviewer
  is required before disabling self-review. Verify the bypass readback in the UI/API; do not assume
  that writing reviewer settings also changes it.
- No custom deployment protection rule is needed. Preserve unrelated environments and protections.
- Store one fresh, narrowly scoped crates.io publishing credential as environment secret
  `CRATES_IO_PUBLISH_TOKEN`. Choose the actual crate scope and finite expiry after inventorying the
  account's tokens, crate ownership and other consumers. Do not create a repository/organization
  copy or expose the value in chat, logs, command arguments or source.

Environment policy matches the run's `GITHUB_REF`, not the checkout SHA or workflow file revision.
Any eligible workflow can request the environment; the owner must review the controller and the
selected release commit before approval. Allowed historical tags containing environment-enabled
controllers can also request approval. Selected-source build scripts run in a credential-capable
job, so the gate is not a sandbox for malicious approved source. Preserve main/tag protections and
keep the proposed history writer App out of their bypass lists.

## Staged cutover

1. Review/merge the code, capture the exact controller SHA and verify its checks. Inventory current
   token ownership, scope, expiry and dependencies through the owner's account without reading or
   displaying its secret value. Inventory repository/organization/environment secret names and all
   current and historical publishing routes. The unrelated `DEPS_APP_PRIVATE_KEY` is not modified by
   this proposal; its existence is not proof that every repository credential is isolated.
2. Under explicit settings approval, create/configure/read back the environment and both typed ref
   policies **before** storing a credential or dispatching these jobs. GitHub can auto-create an
   unprotected environment when a workflow names a missing one. Missing-secret failure is useful but
   does not establish environment protection.
3. Freeze publication during the separately approved credential cutover. Reconcile and drain/cancel
   queued, waiting and active affected runs; capture their identities and partial-release state.
   GitHub snapshots repository secrets when runs are queued. Removing the entry does not revoke a
   token already delivered to a runner.
4. Securely provision the fresh environment-only credential. Remove repository `CRATES_IO_TOKEN` and
   revoke the old value at crates.io, after resolving any other consumers. Remove any other
   historical publishing credential route in the exact approved inventory; do not delete unrelated
   credentials. Verify secret metadata and provider revocation. The secret's absence alone is not
   proof of provider revocation or of token expiry.
5. Complete separately approved nonpublishing acceptance, then authorize an actual release when
   needed. Before accepting a real release, verify publication of every expected crate; offline
   mocks and an authenticated request do not prove full crate scope. No release is authorized by
   this document. Only after the credential prerequisite is verified should the history writer
   App/key setup proceed under its own approval.

## Acceptance and recovery

Offline regression tests read the actual workflow YAML and execute its publishing/check scripts
against a local command stub. They verify source/dependency gates, dry-run selection, fixed dry-run
arguments, missing-credential failure, environment/secret inventory, and command failure handling.
They do not simulate GitHub environment enforcement or crates.io authorization.

Live acceptance requires an approved harmless probe, not dispatching historical release workflows:
those can create GitHub releases/upload assets independently of crates.io authentication. Verify:

- Unapproved jobs cannot start or receive the environment credential. Ordinary branches and
  release-shaped branches are rejected; only the selected branch/tag types are eligible.
- An approved main controller and eligible tag controller can use the environment, and checking out
  an older release commit does not change the run's eligibility. Capture the actual run ref,
  controller SHA, resolved tag/commit and environment-policy readback.
- Old repository credential routes are absent and the old token is revoked, including already
  queued-run exposure. Do not print a token to prove its absence or access.
- Both dry-run paths omit publishing credentials and writes; missing publishing credentials stop
  actual publishing before any selected-source shell command. Include negative controls rather than
  treating a missing fixture as an authorization denial.

On failure, pause publication and preserve draft releases, artifacts and diagnostics. Keep the
protected environment and do not restore the old repository secret or weaken its gate to make an old
workflow work. A replacement credential, revised permissions, alternate controller or release repair
needs its own approval. Do not delete an environment as rollback while credentials/trust still
depend on its name.

Trusted publishing can later remove the long-lived token, but must require the protected environment
and approved crate/workflow identities. It is not configured here. The separate AWS publication-role
narrowing does not grant crates.io or GitHub write access. History-writer Contents permission
remains repository-wide; release-asset mutability and other residual write surfaces still need the
separate [writer setup review](../infra/aws_benchmark/TRUST.md).

References:
[GitHub environments](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments),
[secret timing](https://docs.github.com/en/actions/reference/security/secrets),
[cache token controls](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching#controlling-cache-access-with-cache-mode),
[rerun identity](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs),
[crates.io trusted publishing](https://crates.io/docs/trusted-publishing).
