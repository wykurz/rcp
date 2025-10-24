#!/bin/bash
# Anyhow Error::msg Linter
# Detects usage of anyhow::Error::msg() which destroys error chains
#
# This script uses only standard Unix tools (grep, sed, awk) available in GitHub CI

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Checking for anyhow::Error::msg() usage..."

VIOLATIONS_FOUND=0

# Directories to check
SEARCH_DIRS="common/src rcp/src rlink/src rrm/src rcmp/src filegen/src"

# Find all uses of anyhow::Error::msg(
for dir in $SEARCH_DIRS; do
    if [ -d "$dir" ]; then
        while IFS= read -r file; do
            # Search for anyhow::Error::msg pattern
            while IFS=: read -r line_num line_content; do
                if [ -n "$line_num" ]; then
                    echo -e "${RED}Found violation in $file:$line_num${NC}"
                    echo -e "  ${YELLOW}$line_content${NC}"
                    VIOLATIONS_FOUND=1
                fi
            done < <(grep -n "anyhow::Error::msg(" "$file" 2>/dev/null || true)
        done < <(find "$dir" -name "*.rs" -type f)
    fi
done

if [ $VIOLATIONS_FOUND -eq 1 ]; then
    echo ""
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}ERROR: Found usage of anyhow::Error::msg()${NC}"
    echo -e "${RED}This destroys error chains and hides root causes!${NC}"
    echo ""
    echo "Instead of:"
    echo -e "  ${RED}.map_err(|err| Error::new(anyhow::Error::msg(err), summary))${NC}"
    echo ""
    echo "Use one of these patterns:"
    echo -e "  ${GREEN}// When err is already anyhow::Error (from .with_context()):${NC}"
    echo -e "  ${GREEN}.map_err(|err| Error::new(err, summary))${NC}"
    echo ""
    echo -e "  ${GREEN}// When err is JoinError:${NC}"
    echo -e "  ${GREEN}.map_err(|err| Error::new(err.into(), summary))${NC}"
    echo ""
    echo -e "  ${GREEN}// When err is custom Error type:${NC}"
    echo -e "  ${GREEN}.map_err(|err| Error::new(err.source, summary))${NC}"
    echo ""
    echo "See the error chain preservation fixes for examples."
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
fi

echo -e "${GREEN}✅ No anyhow::Error::msg() usage found!${NC}"
exit 0
