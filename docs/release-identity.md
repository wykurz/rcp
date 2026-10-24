# Release source identity

`release.yml` and `publish.yml` resolve an existing release tag before validation, builds or
publication. The shared `resolve-release-tag.yml` workflow accepts a `v` followed by a digit and
letters, digits, dots, plus signs or hyphens, also checked with `git check-ref-format`. This
includes Cargo prerelease and build-metadata tags such as `v1.2.3-rc.1+build.7`. Branch names, full
refs, raw commit IDs, whitespace and revision expressions are not release-tag inputs.

The resolver fetches the fully qualified `refs/tags/<name>` with the pinned checkout action and
peels lightweight or annotated tags to a commit. It executes only inline Git commands, never scripts
from the checked-out tag. Its token has Contents read permission and is not persisted by checkout. A
missing tag fails even when a branch with the requested name exists.

One resolved commit SHA is passed to the reusable validation workflow and every package-building and
crates.io-publishing checkout. A same-named branch cannot redirect those jobs. Each consumer
directly depends on successful resolution; `skip_validation` skips validation only. `dry_run` still
builds without creating, uploading or publishing releases.

Every GitHub release action also receives that SHA as `target_commitish`, so creating a missing tag
does not default to the repository's default branch. GitHub ignores `target_commitish` when the tag
already exists: this setting does not lock an existing tag or release object.

## Pushes and reruns

For a tag-push run, the resolver requires the requested tag ref to match the event ref and its
commit to match the original event SHA. A moved tag, or an unavailable original event object, stops
the run. Both commit and annotated-tag object SHAs are peeled before comparison.

For manual dispatch, the event SHA identifies the workflow controller, not the requested release
source. An older release tag can therefore be selected from a newer workflow revision. Once
resolved, all consumer jobs use the same commit even if the tag moves later. If a rerun executes the
resolver again, a manual request resolves the tag again; this change does not persist a pin across
independent resolver invocations. Existing successful resolver outputs remain the source of truth
for consumers that reuse them.

## Separate protection requirements

Pinning source checkouts does not make tags or published release assets immutable. A writer that can
create a release tag can trigger the release pipeline, and one that can move an existing tag or edit
a mutable release can still change release metadata or assets; a readback before publication would
not make that change atomic. Restricting creation of release-shaped branch names, protecting tag
creation, updates and deletion, and enabling immutable releases remain separately reviewed live
settings. Immutable releases also require an explicit policy for attempts to re-upload assets after
publication. This code change neither enables those settings nor creates a release or credential.

Old workflow revisions retain their old behavior, including old-ref runs and reruns. Adopt the new
workflow revision for future manual releases; merging this change does not rewrite historical runs.

## Offline regression checks

Run `just release-ref-test`. Tests execute the actual inline resolver scripts against local Git
repositories, including colliding branches, annotated tags and tag movement. They also verify that
all validation, checkout and release-action consumers receive the same outputs and preserve failure,
dry-run and skip-validation behavior. The command runs in local `just lint` and ordinary Rust CI.

Relevant contracts:
[checkout ref selection](https://github.com/actions/checkout/blob/3d3c42e5aac5ba805825da76410c181273ba90b1/src/ref-helper.ts),
[GitHub push events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#push),
and [GitHub release creation](https://docs.github.com/en/rest/releases/releases#create-a-release).
