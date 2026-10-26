# GitHub Workflows

This directory contains GitHub Actions workflows for the RCP Tools project.

## Security conventions

These rules keep fork PRs unable to reach secrets or the release pipeline. Changing any of them is a
security-sensitive change:

- **Never use `pull_request_target` or `workflow_run`.** PR CI runs untrusted code; it must stay in
  the unprivileged `pull_request` context (read-only token, no secrets).
- **Never expose secrets to PR-triggered workflows** (no `secrets:` in `rust.yml` / `validate.yml`,
  no `secrets: inherit`). crates.io / release secrets belong only to tag- and
  `workflow_dispatch`-triggered jobs.
- **SHA-pin every third-party action** with a `# vX` comment. (The `sha_pinning_required` Actions
  policy enforces this once enabled — see repo settings.)
- **Least-privilege `permissions:`** — default `contents: read`; opt up per-job only where needed.
  Every workflow, including each reusable (`workflow_call`) workflow, must declare its own
  `permissions:`; a caller's permissions are only an upper bound (a called workflow can reduce them,
  never elevate).
- **Release tags trigger publishing.** Only repository admins may create `v*` tags (enforced by the
  tag ruleset — see repo settings).

## Workflows

### benchmark-pages.yml

Publishes the project landing page, a trend dashboard under `ci-benchmarks/` from the dedicated
`benchmark-history` branch, and the Markdown-based [reports](../../docs/reports/README.md) under
`reports/`. Every push to `main` and manual requests rebuild the combined site. The push trigger
covers changes to linked documentation as well as report sources. Pull requests validate the report
build and original measurement checksums without deploying. Published rendering code and report
sources come from `main`; historical records are data. The deployment is enabled with the repository
variable `RCP_BENCHMARK_PAGES=true` after configuring Pages to use GitHub Actions. See
[Benchmark dashboard and run results](../../docs/benchmarking.md#view-results) for viewing
instructions, and [Pages setup](../../docs/benchmarking.md#enable-the-historical-site) to enable
publication.

Depot benchmark definitions are removed from refs containing the retirement commit, including PR,
push, schedule and manual benchmark triggers. Old refs and reruns can retain those definitions;
verify live Depot registration and update stale branches before treating spending as stopped. The
ordinary Depot build and test workflow remains available. Local benchmark commands, harness tests
and existing history are retained; AWS qualification is separately opt-in and does not automatically
replace those triggers. No timing threshold blocks a change.

### rust.yml

Runs on every push and pull request to `main`:

- **format**: Checks code formatting with `cargo fmt`
- **lint**: Runs Clippy linter
- **release source identity**: Runs resolver, publication and failure-path regressions inside the
  required lint job
- **documentation**: Builds documentation with strict warnings
- **test**: Runs the full test suite using cargo-nextest

### release.yml

Builds, publishes binary packages, and publishes to crates.io when a tag is pushed. Triggered by
`v*` tag pushes.

**Flow:**

1. Resolves the exact remote tag to one commit, then validates that commit (format, lint, tests)
2. Creates a draft GitHub release
3. Builds packages in parallel:
   - Debian packages (amd64 + arm64)
   - RPM packages (amd64 + arm64)
4. Each build job uploads its package to the draft release
5. Publishes all workspace crates to crates.io
6. Publishes the GitHub release (removes draft status) only after crates.io succeeds

All build and publish jobs check out the resolved commit SHA. Build jobs restore the validated tag
locally for version metadata. Uploads preserve an existing draft; only the final step publishes that
draft. Reruns against an already published mutable release update public assets without restoring
draft state. Manual dispatch accepts a release tag, `dry_run` (build only) and `skip_validation`;
skipping validation does not skip source resolution or downstream success gates. Prerelease versions
are marked as prereleases, while hyphens inside build metadata alone do not mark a stable version as
one. See [Release source identity](../../docs/release-identity.md) for exact source guarantees,
historical-controller compatibility, partial reruns and credential boundaries.

A crates.io failure during a new release leaves its GitHub draft unpublished. After checking the tag
and existing source provenance, prefer **Re-run failed jobs** on the original release run.
`publish.yml` can recover crates.io publication only; it cannot publish that draft. Follow the
[release recovery procedure](../../docs/release-identity.md#recovery) even after a successful
crates-only run.

**Usage:**

Run `just release`, which guides you through the whole process. Because direct pushes to `main` are
disabled, the CHANGELOG and version-bump commits land via pull requests (you merge them with
**Rebase and merge** — click **Update branch** first if GitHub reports the branch is out of date);
the helper opens those PRs for you. The separate release-tag ruleset controls tag creation, updates
and deletion with an administrator bypass. An authorized operator can push the tag to trigger this
workflow:

```bash
git tag v0.24.0
git push origin v0.24.0
```

### publish.yml

Manual-only workflow that publishes crates to crates.io; it does not publish a GitHub draft release.
For a crates.io failure in an environment-enabled `release.yml`, prefer **Re-run failed jobs** on
that original run after the [recovery checks](../../docs/release-identity.md#recovery). Historical
controllers without the environment lose credential access after cutover; use a reviewed modern
controller for those runs. This standalone workflow requires an explicit release tag input (e.g.,
`v0.28.0`), resolves it once, then validates and publishes from that commit SHA. The string-valued
`dry-run` choice defaults to `true`. The selected controller must have build and validation commands
compatible with the chosen source tag; older tags may lack required helpers. Use the current
reviewed controller, and see the
[old-ref credential boundary](../../docs/release-identity.md#separate-protection-requirements)
before changing publication access.

#### Setup Instructions

1. **Review and approve the credential cutover separately**: Follow
   [Publishing credential boundary](../../docs/publishing-credentials.md). This code does not
   create/configure `crates-io`, transfer a token, or revoke the old repository credential.
   Configure its owner approval, disabled administrator bypass and typed branch `main` / tag `v*`
   policies before storing a credential or dispatching a publishing job.

2. **Use an environment-only credential**: After explicit setup approval, store a fresh scoped token
   as `CRATES_IO_PUBLISH_TOKEN` in the `crates-io` environment, never as a repository/organization
   secret. Drain affected queued/running jobs, remove repository `CRATES_IO_TOKEN` and revoke its
   old value at crates.io under the exact cutover plan. Preserve unrelated credentials. Do not copy
   token values into chat or logs.

3. **Usage**:

   Crates.io publishing is part of `release.yml` but requires environment approval. Prefer **Re-run
   failed jobs** on an environment-enabled original release run for recovery; old controllers need
   the modern-controller recovery path after credential cutover. If this standalone workflow is used
   for crates-only recovery, still rerun the failed jobs afterward to publish the GitHub draft.
   Beyond GitHub's 30-day rerun window, manual draft publication requires separate user approval
   after verifying all four expected assets, every crate and their source provenance, as described
   in [Recovery](../../docs/release-identity.md#recovery).

   **Manual re-publish**:
   - Go to Actions → Publish to crates.io → Run workflow
   - Release tag: the tag to publish (e.g., `v0.28.0`)
   - Dry run: `false` to actually publish, `true` (default) to test
   - The workflow resolves the specified tag, checks out its commit SHA, verifies the Cargo.toml
     version matches, validates, and publishes from that commit

#### How It Works

The workflow uses [`cargo-workspaces`](https://crates.io/crates/cargo-workspaces) with the
`--publish-as-is` flag to:

- Publish crates from the current commit without versioning changes
- Automatically discover all workspace members
- Determine correct publishing order based on dependencies
- Handle already-published versions gracefully
- Publish crates in the correct sequence

This approach is much simpler than manually listing crates and eliminates the need to update the
workflow when adding or removing workspace members.

#### Safety Features

- **Version verification**: Ensures release tag matches Cargo.toml version
- **Quality checks**: Runs formatting, clippy, and documentation checks before publishing
- **Automatic dependency ordering**: cargo-workspaces handles dependency resolution
- **Full test suite**: Runs all tests before publishing
- **Dry-run mode**: Can test the workflow without actually publishing
- **Idempotent**: Safely handles already-published versions

#### Troubleshooting

**"already published" error**:

- This means the version already exists on crates.io
- Bump the version in Cargo.toml (workspace.package.version)
- Create a new release with the updated version tag

**Permission denied**:

- Verify the approved `crates-io` environment policy and environment-only `CRATES_IO_PUBLISH_TOKEN`
  metadata. Do not restore the old repository secret to recover an old run.
- Verify the actual token scope covers the intended crates and that it has not expired.
- Dry runs deliberately have no publishing credential and cannot prove real publishing permission.

**Crates.io index delays**:

- cargo-workspaces automatically waits for crates.io to update between dependent crates
- If issues persist, wait a few minutes and re-run the workflow

### validate.yml

Reusable workflow that runs all validation checks (format, lint, tests, documentation). Called by
other workflows to ensure quality gates pass before proceeding.
