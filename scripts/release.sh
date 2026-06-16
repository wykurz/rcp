#!/bin/bash
# Interactive release helper for RCP
# Usage: just release

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Get GitHub repo (owner/name) from origin remote or GITHUB_REPO env var
get_github_repo() {
    if [[ -n "${GITHUB_REPO:-}" ]]; then
        echo "$GITHUB_REPO"
        return
    fi
    local url
    url=$(git remote get-url origin 2>/dev/null) || return 1
    # only process github.com URLs
    if [[ "$url" != *"github.com"* ]]; then
        return 1
    fi
    # handle SSH format: git@github.com:owner/repo.git
    # handle HTTPS format: https://github.com/owner/repo.git
    echo "$url" | sed -E 's#(git@github\.com:|https://github\.com/)##; s#\.git$##'
}

GITHUB_REPO=$(get_github_repo)
if [[ -z "$GITHUB_REPO" ]]; then
    echo -e "${RED}Error: could not determine GitHub repository${NC}"
    echo "Origin remote must be a github.com URL, or set GITHUB_REPO=owner/repo"
    exit 1
fi

# Check that gh CLI is installed (required for release status checks)
if ! command -v gh &> /dev/null; then
    echo -e "${RED}Error: 'gh' command not found.${NC}"
    echo ""
    echo "Please install the GitHub CLI:"
    echo "  https://cli.github.com/"
    exit 1
fi

# Get current version from Cargo.toml [workspace.package] section
get_current_version() {
    awk '/^\[workspace\.package\]/{found=1} found && /^version = /{gsub(/.*= "|"/, ""); print; exit}' Cargo.toml
}

# Check if CHANGELOG has a section for given version
changelog_has_version() {
    local version="$1"
    grep -q "^## \[${version}\]" CHANGELOG.md
}

# Check if git tag exists on remote
remote_tag_exists() {
    local tag="$1"
    git ls-remote --tags origin "$tag" 2>/dev/null | grep -q "refs/tags/${tag}$"
}

# Check if GitHub release exists and is published (not draft)
release_is_published() {
    local tag="$1"
    local is_draft
    is_draft=$(gh release view "$tag" --repo "$GITHUB_REPO" --json isDraft --jq '.isDraft' 2>/dev/null) || return 1
    [[ "$is_draft" == "false" ]]
}

# Get the last release tag
get_last_tag() {
    git describe --tags --abbrev=0 2>/dev/null || echo ""
}

# Find open PRs from this script's release branches (release/changelog-* or
# release/bump-*) that haven't been merged yet. Echoes one "url<TAB>title" line
# per match (nothing if none). Deliberately narrow so an unrelated release/*
# branch can't wedge the release flow.
open_release_pr() {
    # --limit well above any realistic open-PR count; we filter client-side, so a
    # release PR must not fall off the default 30-result page. Emit every match so
    # the caller can surface them all rather than an arbitrary first one.
    gh pr list --repo "$GITHUB_REPO" --state open --limit 200 \
        --json url,title,headRefName \
        --jq '.[] | select((.headRefName | startswith("release/changelog-")) or (.headRefName | startswith("release/bump-"))) | "\(.url)\t\(.title)"'
}

# Abort if the target release branch already exists (a leftover from an
# interrupted run); reusing it would fail or push a diverged history. We only
# reach this when there is no open PR for it — the pending-PR guard handles that.
ensure_branch_free() {
    local branch="$1"
    if git show-ref --verify --quiet "refs/heads/${branch}" \
        || git ls-remote --exit-code --heads origin "${branch}" >/dev/null 2>&1; then
        echo -e "${RED}Error: branch ${branch} already exists (local or remote).${NC}"
        echo "It looks like a leftover from an interrupted release."
        echo "  - If it still holds your release commit, open a PR for it instead:"
        echo "      gh pr create --repo ${GITHUB_REPO} --base main --head ${branch}"
        echo "  - Otherwise delete it and re-run 'just release':"
        echo "      git branch -D ${branch} 2>/dev/null; git push origin --delete ${branch}"
        exit 1
    fi
}

# Build the Claude prompt from template
build_changelog_prompt() {
    local version="$1"
    local last_tag="$2"
    local date="$3"
    local template="$SCRIPT_DIR/changelog-prompt.md"

    if [[ -f "$template" ]]; then
        # use | delimiter since version/tag/date won't contain it
        sed -e "s|\${VERSION}|$version|g" \
            -e "s|\${LAST_TAG}|$last_tag|g" \
            -e "s|\${DATE}|$date|g" \
            "$template"
    else
        echo "Update CHANGELOG.md for release $version (since $last_tag)"
    fi
}

echo -e "${BLUE}${BOLD}"
echo "═══════════════════════════════════════"
echo "  RCP Release Helper"
echo "═══════════════════════════════════════"
echo -e "${NC}"

# Verify we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [[ "$CURRENT_BRANCH" != "main" ]]; then
    echo -e "${RED}Error: must be on 'main' branch to release${NC}"
    echo "Current branch: $CURRENT_BRANCH"
    echo ""
    echo "Switch to main: git checkout main"
    exit 1
fi

# Require a clean working tree up-front. Every state operates on main as a known
# baseline (State 3's version bump even runs 'git add -A'), so stray uncommitted
# changes must not be carried into a release branch — and we always want the
# fast-forward below to run, not be skipped because the tree is dirty.
if [[ -n "$(git status --porcelain)" ]]; then
    echo -e "${RED}Error: working tree has uncommitted changes${NC}"
    echo ""
    git status --short
    echo ""
    echo "Commit, stash, or discard them before running the release helper."
    exit 1
fi

# Fetch tags and main branch to ensure we have the latest release info
# use --force to update local tags that may differ from remote
echo -e "Fetching from origin..."
git fetch --tags --force --quiet origin main
echo ""

# Sync local main with origin/main. After a release PR is merged (rebase),
# origin/main advances; fast-forward catches local main up so the right commit
# gets tagged. Fail loudly instead of inventing a merge commit (the linear-history
# rule on main would reject it anyway).
git merge --ff-only origin/main --quiet || {
    echo -e "${RED}Error: local main has diverged from origin/main; cannot fast-forward.${NC}"
    echo "Reconcile manually (e.g. 'git reset --hard origin/main' if you have no local-only commits)."
    exit 1
}

# A fast-forward is a silent no-op when local main is *ahead* of origin/main (an
# unpushed local commit), so it does not by itself guarantee the two match.
# Require exact equality: releases are cut from origin/main (everything lands via
# PR), and tagging an unpushed commit would publish from an unreviewed revision.
if [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]]; then
    echo -e "${RED}Error: local main is ahead of origin/main (unpushed commits)${NC}"
    echo ""
    echo "Local HEAD:  $(git rev-parse HEAD)"
    echo "origin/main: $(git rev-parse origin/main)"
    echo ""
    echo "Releases must be cut from origin/main. After saving any work you need:"
    echo "  git reset --hard origin/main"
    exit 1
fi

CURRENT_VERSION=$(get_current_version)
TAG="v${CURRENT_VERSION}"
LAST_TAG=$(get_last_tag)

echo -e "Current version: ${GREEN}${CURRENT_VERSION}${NC}"
echo -e "Last release:    ${GREEN}${LAST_TAG:-none}${NC}"
echo ""

# Direct pushes to main are disabled, so the CHANGELOG and version-bump commits
# land via PR. If one is still open, the only valid next action is to merge it —
# stop here so re-runs don't open duplicate PRs.
if ! PENDING_PRS=$(open_release_pr); then
    echo -e "${RED}Error: failed to query GitHub for open release PRs.${NC}"
    echo "Check your GitHub CLI auth and access: gh auth status"
    exit 1
fi
if [[ -n "$PENDING_PRS" ]]; then
    echo -e "${YELLOW}${BOLD}State: release PR awaiting merge${NC}"
    echo ""
    echo "Open release PR(s) must be merged before continuing:"
    while IFS=$'\t' read -r pr_url pr_title; do
        echo -e "  ${BOLD}${pr_title}${NC}"
        echo "  ${pr_url}"
    done <<< "$PENDING_PRS"
    echo ""
    echo -e "Merge with ${BOLD}Rebase and merge${NC} once checks pass, then re-run 'just release'."
    echo "(If GitHub says a branch is out of date with main, click 'Update branch' first.)"
    exit 0
fi

# Determine state and act
if ! changelog_has_version "$CURRENT_VERSION"; then
    # ═══════════════════════════════════════════════════════════════════
    # State 1: CHANGELOG needs update for current version
    # ═══════════════════════════════════════════════════════════════════
    echo -e "${YELLOW}${BOLD}State: CHANGELOG needs update${NC}"
    echo ""
    echo "CHANGELOG.md doesn't have a section for [${CURRENT_VERSION}] yet."
    echo "This means you're ready to finalize release notes."
    echo ""
    echo -e "${BOLD}Proposed action:${NC}"
    echo "  1. Invoke Claude to update CHANGELOG.md with release notes"
    echo "  2. Open a PR with the CHANGELOG update for you to review and merge"
    echo ""
    echo "You can edit the changes afterward if needed."
    echo ""
    read -p "Proceed? [Y/n] " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Aborted."
        exit 0
    fi

    echo ""
    echo -e "${GREEN}Invoking Claude to update CHANGELOG...${NC}"
    echo ""

    # check that claude CLI is installed
    if ! command -v claude &> /dev/null; then
        echo -e "${RED}Error: 'claude' command not found.${NC}"
        echo ""
        echo "Please install Claude Code CLI:"
        echo "  https://docs.anthropic.com/en/docs/claude-code"
        exit 1
    fi

    DATE=$(date +%Y-%m-%d)
    PROMPT=$(build_changelog_prompt "$CURRENT_VERSION" "$LAST_TAG" "$DATE")

    # Run Claude to update the changelog
    claude "$PROMPT"

    echo ""

    # verify only CHANGELOG.md was modified
    CHANGED_FILES=$(git diff --name-only)
    if [[ -z "$CHANGED_FILES" ]]; then
        echo -e "${YELLOW}No changes were made to CHANGELOG.md${NC}"
        echo "You may need to update it manually."
        exit 0
    elif [[ "$CHANGED_FILES" != "CHANGELOG.md" ]]; then
        echo -e "${RED}Error: unexpected files were modified:${NC}"
        echo "$CHANGED_FILES"
        echo ""
        echo "Only CHANGELOG.md should be modified. Please review and reset unwanted changes."
        exit 1
    fi

    echo -e "${GREEN}CHANGELOG updated.${NC}"
    echo ""

    # Show diff and ask to open a PR
    echo -e "${BOLD}Changes made:${NC}"
    git diff CHANGELOG.md || true
    echo ""

    read -p "Open a PR with this CHANGELOG update? [Y/n] " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Aborted. The CHANGELOG edit is left uncommitted in your working tree;"
        echo "a re-run won't resume from here until you commit it or discard it"
        echo "(git checkout -- CHANGELOG.md)."
        exit 0
    fi

    BRANCH="release/changelog-${TAG}"
    ensure_branch_free "$BRANCH"
    echo ""
    echo -e "${GREEN}Creating branch ${BRANCH} and opening PR...${NC}"
    git checkout -b "$BRANCH"
    git add CHANGELOG.md
    git commit -m "Update CHANGELOG for ${TAG}"
    git push -u origin "$BRANCH"

    if ! PR_URL=$(gh pr create --repo "$GITHUB_REPO" --base main --head "$BRANCH" \
        --title "Update CHANGELOG for ${TAG}" \
        --body "Finalize release notes for ${TAG}. Merge with **Rebase and merge** once checks pass (click **Update branch** first if GitHub says it's out of date), then run \`just release\` to create and push the tag."); then
        git checkout main --quiet
        echo ""
        echo -e "${RED}Branch ${BRANCH} was pushed, but 'gh pr create' failed.${NC}"
        echo "Your commit is safe on that branch — don't delete it. Open the PR manually:"
        echo "  gh pr create --repo ${GITHUB_REPO} --base main --head ${BRANCH}"
        exit 1
    fi

    # return to main so the working tree is clean for the next step
    git checkout main --quiet

    echo ""
    echo -e "${GREEN}${BOLD}CHANGELOG PR opened:${NC} ${PR_URL}"
    echo ""
    echo -e "${BOLD}Next steps:${NC}"
    echo "  1. Wait for checks, then merge the PR with 'Rebase and merge'"
    echo "  2. Run 'just release' again to create and push the ${TAG} tag"

elif ! remote_tag_exists "$TAG"; then
    # ═══════════════════════════════════════════════════════════════════
    # State 2: CHANGELOG updated, ready to tag and release
    # ═══════════════════════════════════════════════════════════════════
    echo -e "${YELLOW}${BOLD}State: Ready to release${NC}"
    echo ""

    # verify working directory is clean
    if [[ -n "$(git status --porcelain)" ]]; then
        echo -e "${RED}Error: working directory has uncommitted changes${NC}"
        echo ""
        git status --short
        echo ""
        echo "Commit or stash your changes before releasing."
        exit 1
    fi

    # local main was fast-forwarded to origin/main at startup, so HEAD is exactly
    # the commit that will be tagged — there is nothing to push to main here.

    # check if local tag already exists (from a previous failed push attempt)
    if git rev-parse "$TAG" >/dev/null 2>&1; then
        echo -e "${YELLOW}Warning: local tag ${TAG} already exists${NC}"
        echo ""
        echo "This may be from a previous failed push attempt."
        read -p "Delete local tag and recreate? [Y/n] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            echo "Aborted. You can manually delete the tag with: git tag -d ${TAG}"
            exit 0
        fi
        git tag -d "$TAG"
        echo ""
    fi

    # The release notes come from the CHANGELOG section for this version (see
    # release.yml). The startup fast-forward may have advanced main past the commit
    # that finalized those notes; anything landed since would ship in the tag
    # without necessarily appearing in the published notes. Warn before tagging.
    # Use the -S pickaxe to find the commit that introduced "## [VERSION]", not
    # merely the last commit to touch CHANGELOG.md — a later unrelated edit to
    # another section must not move the boundary forward and hide commits.
    CHANGELOG_COMMIT=$(git log -1 --format=%H -S"## [${CURRENT_VERSION}]" -- CHANGELOG.md)
    if [[ -n "$CHANGELOG_COMMIT" && "$CHANGELOG_COMMIT" != "$(git rev-parse HEAD)" ]]; then
        echo -e "${YELLOW}${BOLD}Warning: main advanced past the CHANGELOG update for ${CURRENT_VERSION}.${NC}"
        echo ""
        echo "These commits landed after the release notes were finalized and would"
        echo "ship in tag ${TAG} without necessarily appearing in the notes:"
        git --no-pager log --oneline "${CHANGELOG_COMMIT}..HEAD"
        echo ""
        echo "Re-run the CHANGELOG step to cover them, or proceed if they are"
        echo "release-irrelevant (e.g. CI/docs/tests)."
        read -p "Tag ${TAG} at the current HEAD anyway? [y/N] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Aborted."
            exit 0
        fi
        echo ""
    fi

    echo "CHANGELOG has been updated for [${CURRENT_VERSION}],"
    echo "but git tag ${TAG} hasn't been pushed to origin yet."
    echo ""
    echo -e "${BOLD}Proposed action:${NC}"
    echo "  1. Create git tag ${TAG}"
    echo "  2. Push tag to origin (triggers release workflow)"
    echo ""
    echo "This will trigger the release workflow which:"
    echo "  - Creates a draft GitHub release with notes from CHANGELOG"
    echo "  - Builds and attaches deb/rpm packages (amd64 + arm64)"
    echo "  - Publishes the release (triggers crates.io publication)"
    echo ""
    read -p "Create and push tag ${TAG}? [Y/n] " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Aborted."
        exit 0
    fi

    echo ""
    echo -e "${GREEN}Creating tag ${TAG}...${NC}"
    git tag "$TAG"

    echo -e "${GREEN}Pushing tag to origin...${NC}"
    if ! git push origin "$TAG"; then
        echo ""
        echo -e "${RED}Error: failed to push tag to origin${NC}"
        echo "Deleting local tag to allow retry..."
        git tag -d "$TAG"
        exit 1
    fi

    echo ""
    echo -e "${GREEN}${BOLD}Tag ${TAG} pushed!${NC}"
    echo ""
    echo "Check workflow status at:"
    echo "  https://github.com/${GITHUB_REPO}/actions/workflows/release.yml"
    echo ""
    echo "After the release is published, run 'just release' again to bump version."
    exit 0

elif ! release_is_published "$TAG"; then
    # ═══════════════════════════════════════════════════════════════════
    # State 2.5: Tag pushed, waiting for release workflow to complete
    # ═══════════════════════════════════════════════════════════════════
    echo -e "${YELLOW}${BOLD}State: Release in progress${NC}"
    echo ""
    echo "Tag ${TAG} has been pushed, but the release is not yet published."
    echo "The release workflow may still be running or may have failed."
    echo ""
    echo "Check workflow status at:"
    echo "  https://github.com/${GITHUB_REPO}/actions/workflows/release.yml"
    echo ""
    echo "Once the release is published, run 'just release' again to bump version."
    exit 0

else
    # ═══════════════════════════════════════════════════════════════════
    # State 3: Release complete, bump to next version
    # ═══════════════════════════════════════════════════════════════════
    echo -e "${GREEN}${BOLD}State: Release ${TAG} complete!${NC}"
    echo ""
    echo "The release ${TAG} is published. Time to bump version for next development cycle."
    echo ""

    # Push to Radicle if rad is available
    if command -v rad &> /dev/null; then
        echo -e "${BOLD}Pushing to Radicle...${NC}"
        # ensure rad node is running
        if ! rad node status &> /dev/null; then
            echo "Starting rad node..."
            rad node start
            sleep 2
        fi
        if git push rad; then
            echo -e "${GREEN}Pushed to Radicle.${NC}"
        else
            echo -e "${YELLOW}Warning: failed to push to Radicle (continuing anyway)${NC}"
        fi
        echo ""
    fi

    # validate version format before parsing
    if ! [[ "$CURRENT_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo -e "${RED}Error: version '$CURRENT_VERSION' is not in MAJOR.MINOR.PATCH format${NC}"
        exit 1
    fi

    # Parse current version
    IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

    NEXT_PATCH="$MAJOR.$MINOR.$((PATCH + 1))"
    NEXT_MINOR="$MAJOR.$((MINOR + 1)).0"
    NEXT_MAJOR="$((MAJOR + 1)).0.0"

    echo -e "${BOLD}What type of release do you expect next?${NC}"
    echo ""
    echo -e "  1) Minor  ${NEXT_MINOR}  - new features ${YELLOW}[default]${NC}"
    echo -e "  2) Patch  ${NEXT_PATCH}  - bug fixes only"
    echo -e "  3) Major  ${NEXT_MAJOR}  - breaking changes"
    echo ""
    read -p "Select [1/2/3]: " -r

    case $REPLY in
        2) NEXT_VERSION="$NEXT_PATCH" ;;
        3) NEXT_VERSION="$NEXT_MAJOR" ;;
        *) NEXT_VERSION="$NEXT_MINOR" ;;
    esac

    echo ""
    echo -e "Will bump version: ${CURRENT_VERSION} → ${GREEN}${BOLD}${NEXT_VERSION}${NC}"
    echo ""
    echo "This will update:"
    echo "  - Cargo.toml (workspace version + internal deps)"
    echo "  - flake.nix"
    echo ""
    read -p "Proceed? [Y/n] " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Aborted."
        exit 0
    fi

    echo ""
    if ! ./update-version.sh "$CURRENT_VERSION" "$NEXT_VERSION"; then
        echo -e "${RED}Error: update-version.sh failed${NC}"
        exit 1
    fi

    # verify the version was actually updated
    NEW_VERSION=$(get_current_version)
    if [[ "$NEW_VERSION" != "$NEXT_VERSION" ]]; then
        echo -e "${RED}Error: version update verification failed${NC}"
        echo "Expected: $NEXT_VERSION"
        echo "Got: $NEW_VERSION"
        exit 1
    fi

    echo ""
    echo -e "${GREEN}Version bumped to ${NEXT_VERSION}${NC}"
    echo ""

    # Show diff
    echo -e "${BOLD}Changes made:${NC}"
    git diff --stat
    echo ""

    read -p "Open a PR with this version bump? [Y/n] " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Aborted. The version bump is left uncommitted in your working tree;"
        echo "a re-run won't resume from here until you commit it or discard it"
        echo "(git checkout -- Cargo.toml Cargo.lock flake.nix)."
        exit 0
    fi

    BRANCH="release/bump-v${NEXT_VERSION}"
    ensure_branch_free "$BRANCH"
    echo ""
    echo -e "${GREEN}Creating branch ${BRANCH} and opening PR...${NC}"
    git checkout -b "$BRANCH"
    git add -A
    git commit -m "Bump version to ${NEXT_VERSION}"
    git push -u origin "$BRANCH"

    if ! PR_URL=$(gh pr create --repo "$GITHUB_REPO" --base main --head "$BRANCH" \
        --title "Bump version to ${NEXT_VERSION}" \
        --body "Bump workspace version ${CURRENT_VERSION} → ${NEXT_VERSION} for the next development cycle. Merge with **Rebase and merge** (click **Update branch** first if GitHub says it's out of date)."); then
        git checkout main --quiet
        echo ""
        echo -e "${RED}Branch ${BRANCH} was pushed, but 'gh pr create' failed.${NC}"
        echo "Your commit is safe on that branch — don't delete it. Open the PR manually:"
        echo "  gh pr create --repo ${GITHUB_REPO} --base main --head ${BRANCH}"
        exit 1
    fi

    # return to main so the working tree is clean
    git checkout main --quiet

    echo ""
    echo -e "${GREEN}${BOLD}Version bump PR opened:${NC} ${PR_URL}"
    echo ""
    echo -e "${BOLD}Next steps:${NC}"
    echo "  1. Wait for checks, then merge the PR with 'Rebase and merge'"
    echo "  2. That completes the ${TAG} release 🎉"
fi
