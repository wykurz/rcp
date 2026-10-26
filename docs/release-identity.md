# Release source identity

`release.yml` and `publish.yml` resolve an existing release tag before validation, builds or
publication. The shared `resolve-release-tag.yml` workflow accepts a `v` followed by a digit and
letters, digits, dots, plus signs or hyphens, also checked with `git check-ref-format`. This
includes Cargo prerelease and build-metadata tags such as `v1.2.3-rc.1+build.7`. Branch names, full
refs, raw commit IDs, whitespace and revision expressions are not release-tag inputs. The published
source must still have the matching Cargo version; the name check alone is not a SemVer validator.

RPM packaging uses a package-specific version mapping after checking that the tag matches the
workspace Cargo version: the prerelease separator becomes `~`, and hyphens within prerelease or
build identifiers become `_`. For example, `1.2.3-rc.1+build-with-hyphen.7` becomes
`1.2.3~rc.1+build_with_hyphen.7` in the RPM Version field. Versions without these hyphens, including
`+build` metadata, are unchanged. RPM places mapped prereleases before their corresponding stable
version, but does not preserve every SemVer ordering rule: for example, `1.2.3-rc-1` and
`1.2.3-rc.1` compare equal after mapping, and build metadata can affect ordering. Cargo versions,
release tags, archive names and embedded binary metadata keep their original identity.

The resolver checks out `refs/tags/<name>`, then compares the held local tag object's ID with an
exactly named remote advertisement. Git can otherwise resolve a missing tag through similarly shaped
refs even when a fetch requests a qualified ref. The comparison includes annotated-tag object
identity before peeling to a commit, and that held object's commit must equal `HEAD`. Missing,
moved, deleted or replaced tags observed during this check fail resolution. Lightweight and
annotated tags are supported. This is a source observation and a held commit pin, not a lock on the
remote tag after the observation.

Resolution executes inline Git commands, never scripts from the checked-out tag. Its token has
Contents read permission and is not persisted by checkout. The remote advertisement read is
anonymous against this public repository. A refused or failed read stops resolution with a distinct
unavailable-read error; retry resolution after reads recover. That outage is not evidence that the
tag changed. A successful read whose exact tag is missing or differs from checkout instead reports a
tag mismatch. One resolved commit SHA is passed to validation and every build and crates.io
checkout. Each consumer directly depends on successful resolution. Build jobs restore the validated
tag locally at the verified commit for `git describe` version metadata; they neither fetch a changed
tag nor push one.

GitHub release actions use the validated tag name and omit `target_commitish`. The tag must already
exist. A target override is not a checkout identity control and can trigger GitHub's workflow-write
permission check when an action updates release metadata. The pinned release action also resends a
stored release target on updates: omission preserves an existing target, but cannot repair an
already incompatible target. Such a failure needs separate inspection, not broader token access.

For a new or existing draft release, package uploads preserve draft state. Only the final release
job publishes that draft, after every package build, the isolated artifact upload job and crates.io
publication succeed. Package builds are read-only; the upload job executes only controller-owned
staging code, accepts bounded immutable artifacts, and never executes their contents. See the
[artifact boundary](publishing-credentials.md#package-artifact-boundary) for identity and retention
limits. A crates.io failure leaves the release a draft; prefer **Re-run failed jobs** on the
original release run after the [recovery checks](#recovery) below. Upload reruns against an already
published mutable release do not restore draft state: those asset updates remain public and need the
separately reviewed recovery policy below. `skip_validation` bypasses only the validation job;
explicit dependency gates still require resolution and downstream work to succeed. `dry_run` builds
without creating, uploading or publishing releases. Prerelease status comes from the version before
any `+` metadata, and changelog headings are matched literally.

## Pushes and reruns

For a tag-push run, resolution requires the requested tag ref to match the event ref and its commit
to match the original event SHA. An unavailable original event object or a different commit stops
that resolver invocation with a diagnostic. Both commit and annotated-tag event SHAs are peeled
before comparison.

For manual dispatch, the event SHA identifies the workflow controller, not the requested release
source. The resolver can pin an older tag, but the selected controller's validation and build
commands must be compatible with that source tree. This is not a guarantee that current workflows
can build every historical release: older tags lack helpers required by current validation.

Consumers reuse the resolved commit if the remote tag later moves. A partial rerun that reuses a
successful resolver output also keeps that pin; only rerunning resolution observes the remote tag
again. A manual resolver rerun may select a different commit. A push resolver rerun still must match
the original event. Repointing a `v*` tag is itself a tag push and starts a new release run for the
new commit. If a tag changes while a release is active, cancel every active run for that tag,
including the newly triggered run. Inspect the source provenance of existing draft assets and
crates.io versions before choosing recovery; only then start one complete run for the intended
source. Source pinning does not atomically bind later release API writes to the current remote tag.

## Recovery

Before recovering a failed release, compare its resolved commit with the current tag and inspect
what already exists: the draft or published release, all four expected ZIP assets (deb and rpm
bundles for amd64 and arm64), and every workspace crate version. Bind assets to their originating
build/run and check each published crate's `.cargo_vcs_info.json` `git.sha1` against the intended
source commit. `check-crates-published.sh` verifies version existence only; it does not prove source
provenance. Stop and reconcile any changed tag, uncertain asset source or mismatched crate before
publication.

For a crates.io failure with an unchanged tag and consistent source provenance, prefer **Re-run
failed jobs** on the original `release.yml` run. It retains the successful resolver's pin, retries
crates.io publication, checks every crate version, and then publishes the draft. Standalone
`publish.yml` publishes **crates only**; it does not publish the GitHub draft. Even after using it
for crates-only recovery, rerun the failed jobs on the original release run to complete draft
publication.

GitHub permits [reruns for 30 days after the initial run][rerun-window]. Beyond that window,
manually publishing the draft requires separate user approval after confirming all four expected
assets and every crate are present and their source provenance matches the intended commit and
current tag. Missing or inconsistent evidence holds publication; a successful crates-only run is not
enough.

[rerun-window]: https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs

## Separate protection requirements

See [Publishing credential boundary](publishing-credentials.md) for the proposed environment-only
credential cutover, historical-controller limitations and separately approved live acceptance.

Pinning checkouts does not make tags or published release assets immutable. Repository policy is
separate from this code. At the October 25, 2026 read-only check, the active `release-tags` ruleset
restricted creation, update and deletion of `refs/tags/v*`, with an administrator bypass. The active
`main` ruleset required `validate / lint-and-docs`, which now includes these regression tests. These
observations are not permission to change either ruleset.

A writer allowed to create release tags can trigger publishing, and one allowed to move tags or edit
mutable releases can alter release metadata or assets. Restricting release-shaped branch creation,
changing tag policy, and enabling immutable releases require separate review. Immutable releases
also need an explicit recovery policy for asset re-uploads after publication. This change does not
enable settings, create credentials or publish a release.

Old workflow revisions retain their old behavior, including old-ref dispatches and reruns. The
person dispatching a manual workflow can choose an old controller ref; recommending the new ref is
not an enforced credential boundary. In particular, the existing repository-scoped `CRATES_IO_TOKEN`
remains reachable by historical publication workflows with weaker source checks. Tag rules alone do
not restrict arbitrary branch names accepted by those old workflows. Before provisioning another
repository writer, including the
[AWS benchmark history writer App or key](../infra/aws_benchmark/TRUST.md#approval-and-acceptance-sequence),
complete a separate review of moving release credentials to an appropriately restricted environment
or a trusted-publishing policy, including old-ref rejection and recovery tests. Such credential or
policy changes require separate approval. This PR does not move or revoke that credential or create
an environment.

## Offline regression checks

Run `just release-ref-test`. The committed workflow regression suite executes the actual resolver
scripts and workflow-selected refs against local Git repositories. Negative cases cover shadow refs,
moved, deleted and re-annotated tags, wrong pins, unavailable reads, hostile tag scripts and failed,
skipped or cancelled dependencies. It also checks caller inputs, shell/environment boundaries, the
dependency graph, draft upload settings, version metadata and prerelease/changelog behavior. The
recipe also runs the RPM version mapping's unit, CLI and workflow packaging checks.

The expression model is limited to constructs used here; it is not a full GitHub Actions runner.
Checks bind the checkout adapter behavior and the exact checkout and release-action pins; they do
not execute or emulate the release action's API behavior. Mutation scripts and release-action mocks
used during review are scratch artifacts, not committed tests, and this command does not run them.
These offline checks do not replace a separately authorized live release acceptance test.

The command runs in local `just lint` and the existing required `validate / lint-and-docs` job. It
needs Git 2.30 or later, Bash, Python 3.11 or later and PyYAML. The CI job supplies these
dependencies. Locally, Git must be available on the host PATH; the default `nix develop` shell does
not provide Git.

Relevant contracts:
[checkout ref selection](https://github.com/actions/checkout/blob/3d3c42e5aac5ba805825da76410c181273ba90b1/src/ref-helper.ts),
[remote ref advertisements](https://git-scm.com/docs/git-ls-remote),
[GitHub expressions](https://docs.github.com/en/actions/reference/workflows-and-actions/expressions),
[GitHub push events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#push),
and [GitHub release updates](https://docs.github.com/en/rest/releases/releases#update-a-release).
