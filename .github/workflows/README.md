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
Builds and publishes binary packages when a tag is pushed. Triggered by `v*` tag pushes.

**Flow:**
1. Runs validation (format, lint, tests)
2. Creates a draft GitHub release
3. Builds packages in parallel:
   - Debian packages (amd64 + arm64)
   - RPM packages (amd64 + arm64)
4. Uploads all packages to the draft release
5. Publishes the release (triggers publish.yml)

**Usage:**
```bash
git tag v0.24.0
git push origin v0.24.0
```

Or use `just release` which guides you through the process.

### publish.yml
Publishes all crates to crates.io when a release is published.

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

   **Automatic publishing on release**:
   - Push a version tag: `git tag v0.24.0 && git push origin v0.24.0`
   - The release.yml workflow creates the GitHub release
   - When the release is published, this workflow will automatically:
     - Verify the tag matches the version in Cargo.toml
     - Check formatting, run clippy, and build documentation
     - Run the full test suite
     - Publish all workspace crates to crates.io in dependency order
     - Skip already published versions automatically

   **Manual dry-run testing**:
   - Go to Actions → Publish to crates.io → Run workflow
   - Select branch: `main`
   - Dry run: `true`
   - This will test the entire flow without actually publishing

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
