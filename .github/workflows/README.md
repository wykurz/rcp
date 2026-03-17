# GitHub Workflows

This directory contains GitHub Actions workflows for the RCP Tools project.

## Workflows

### rust.yml
Runs on every push and pull request to `main`:
- **format**: Checks code formatting with `cargo fmt`
- **lint**: Runs Clippy linter
- **documentation**: Builds documentation with strict warnings
- **test**: Runs the full test suite using cargo-nextest

### release.yml
Builds, publishes binary packages, and publishes to crates.io when a tag is pushed. Triggered by `v*` tag pushes.

**Flow:**
1. Runs validation (format, lint, tests) against the release tag
2. Creates a draft GitHub release
3. Builds packages in parallel:
   - Debian packages (amd64 + arm64)
   - RPM packages (amd64 + arm64)
4. Each build job uploads its package to the draft release
5. Publishes all workspace crates to crates.io
6. Publishes the GitHub release (removes draft status) only after crates.io succeeds

All build and publish jobs check out the release tag, ensuring consistency even on manual workflow_dispatch reruns.

**Usage:**
```bash
git tag v0.24.0
git push origin v0.24.0
```

Or use `just release` which guides you through the process.

### publish.yml
Manual-only workflow for re-publishing to crates.io (e.g., after a failed release). Requires an explicit release tag input (e.g., `v0.28.0`), validates and publishes from that exact tagged commit.

#### Setup Instructions

1. **Create a crates.io API token**:
   - Go to https://crates.io/settings/tokens
   - Click "New Token"
   - Give it a descriptive name (e.g., "GitHub Actions - RCP Tools")
   - Grant it the `publish-update` scope
   - Copy the generated token

2. **Add the token to GitHub Secrets**:
   - Go to your repository on GitHub
   - Navigate to Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `CRATES_IO_TOKEN`
   - Value: Paste the token from step 1
   - Click "Add secret"

3. **Usage**:

   Crates.io publishing happens automatically as part of release.yml. This workflow is only needed for manual recovery (e.g., if the crates.io step failed during a release).

   **Manual re-publish**:
   - Go to Actions → Publish to crates.io → Run workflow
   - Release tag: the tag to publish (e.g., `v0.28.0`)
   - Dry run: `false` to actually publish, `true` (default) to test
   - The workflow checks out the specified tag, verifies the Cargo.toml version matches, validates, and publishes from that exact commit

#### How It Works

The workflow uses [`cargo-workspaces`](https://crates.io/crates/cargo-workspaces) with the `--publish-as-is` flag to:
- Publish crates from the current commit without versioning changes
- Automatically discover all workspace members
- Determine correct publishing order based on dependencies
- Handle already-published versions gracefully
- Publish crates in the correct sequence

This approach is much simpler than manually listing crates and eliminates the need to update the workflow when adding or removing workspace members.

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
- Verify CRATES_IO_TOKEN secret is set correctly
- Ensure the token has `publish-update` scope
- Check token hasn't expired

**Crates.io index delays**:
- cargo-workspaces automatically waits for crates.io to update between dependent crates
- If issues persist, wait a few minutes and re-run the workflow

### validate.yml
Reusable workflow that runs all validation checks (format, lint, tests, documentation).
Called by other workflows to ensure quality gates pass before proceeding.
