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

# Get current version from Cargo.toml [workspace.package] section
get_current_version() {
    awk '/^\[workspace\.package\]/{found=1} found && /^version = /{gsub(/.*= "|"/, ""); print; exit}' Cargo.toml
}

# Check if CHANGELOG has a section for given version
changelog_has_version() {
    local version="$1"
    grep -q "^## \[${version}\]" CHANGELOG.md
}

# Check if git tag exists
tag_exists() {
    local tag="$1"
    git tag -l "$tag" | grep -q "^${tag}$"
}

# Get the last release tag
get_last_tag() {
    git describe --tags --abbrev=0 2>/dev/null || echo ""
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

# Fetch tags to ensure we have the latest release info
echo -e "Fetching tags from origin..."
git fetch --tags --quiet
echo ""

CURRENT_VERSION=$(get_current_version)
TAG="v${CURRENT_VERSION}"
LAST_TAG=$(get_last_tag)

echo -e "Current version: ${GREEN}${CURRENT_VERSION}${NC}"
echo -e "Last release:    ${GREEN}${LAST_TAG:-none}${NC}"
echo ""

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
    echo "  2. Create a commit with the CHANGELOG update"
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
    echo -e "${GREEN}CHANGELOG updated.${NC}"
    echo ""

    # Show diff and ask to commit
    echo -e "${BOLD}Changes made:${NC}"
    git diff CHANGELOG.md || true
    echo ""

    read -p "Create commit for CHANGELOG update? [Y/n] " -n 1 -r
    echo

    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        git add CHANGELOG.md
        git commit -m "Update CHANGELOG for v${CURRENT_VERSION}"
        echo ""
        echo -e "${GREEN}Committed CHANGELOG update.${NC}"
    fi

    echo ""
    echo -e "${BOLD}Next steps:${NC}"
    echo "  1. Review the commit: git show"
    echo "  2. Push to create PR: git push"
    echo "  3. After PR merges, create GitHub release for ${TAG}"
    echo "  4. Run 'just release' again to bump version"

elif ! tag_exists "$TAG"; then
    # ═══════════════════════════════════════════════════════════════════
    # State 2: CHANGELOG updated, waiting for GitHub release
    # ═══════════════════════════════════════════════════════════════════
    echo -e "${YELLOW}${BOLD}State: Awaiting GitHub release${NC}"
    echo ""
    echo "CHANGELOG has been updated for [${CURRENT_VERSION}],"
    echo "but git tag ${TAG} doesn't exist yet."
    echo ""
    echo -e "${RED}${BOLD}Action required:${NC} Create a GitHub release"
    echo ""
    echo "  1. Go to: https://github.com/wykurz/rcp/releases/new"
    echo "  2. Tag: ${TAG}"
    echo "  3. Title: ${TAG}"
    echo "  4. Copy release notes from CHANGELOG.md"
    echo ""
    echo "This will trigger:"
    echo "  - Build and attach deb/rpm packages"
    echo "  - Publish to crates.io"
    echo ""
    echo "After the release is created, run 'just release' again to bump version."
    exit 0

else
    # ═══════════════════════════════════════════════════════════════════
    # State 3: Release complete, bump to next version
    # ═══════════════════════════════════════════════════════════════════
    echo -e "${GREEN}${BOLD}State: Release ${TAG} complete!${NC}"
    echo ""
    echo "The git tag ${TAG} exists. Time to bump version for next development cycle."
    echo ""

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

    read -p "Create commit for version bump? [Y/n] " -n 1 -r
    echo

    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        git add -A
        git commit -m "Bump version to ${NEXT_VERSION}"
        echo ""
        echo -e "${GREEN}Committed version bump.${NC}"
    fi

    echo ""
    echo -e "${BOLD}Next steps:${NC}"
    echo "  1. Push changes: git push"
    echo "  2. Start working on new features!"
fi
