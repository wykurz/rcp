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
SEARCH_DIRS="common/src congestion/src filegen/src rchm/src rcmp/src rcp/src remote/src rlink/src rrm/src throttle/src"

# Detect usages outside of comments
check_file() {
    local file="$1"
    awk '
    function ltrim(str) {
        sub(/^[[:space:]]+/, "", str)
        return str
    }
    BEGIN {
        in_block = 0
    }
    {
        line = $0
        trimmed = ltrim(line)

        if (in_block) {
            if (index(trimmed, "*/")) {
                trimmed = substr(trimmed, index(trimmed, "*/") + 2)
                trimmed = ltrim(trimmed)
                in_block = 0
            } else {
                next
            }
        }

        while ((start = index(trimmed, "/*")) > 0) {
            before = substr(trimmed, 1, start - 1)
            rest = substr(trimmed, start + 2)
            end = index(rest, "*/")
            if (end > 0) {
                rest = substr(rest, end + 2)
                trimmed = before rest
            } else {
                trimmed = before
                in_block = 1
                break
            }
        }

        trimmed = ltrim(trimmed)
        if (trimmed ~ /^\/\// || trimmed ~ /^$/) {
            next
        }
        if (trimmed ~ /anyhow::Error::msg[[:space:]]*\(/) {
            print NR ":" line
        }
    }
    ' "$file"
}

# Find all uses of anyhow::Error::msg outside comments
for dir in $SEARCH_DIRS; do
    if [ -d "$dir" ]; then
        while IFS= read -r file; do
            # awk's own failure — a syntax error in the program above, an unreadable file — produces
            # empty output, which is indistinguishable from a clean file, so the check would report
            # success for a scan that never ran. Fatal rather than counted as a violation: a linter
            # that did not run is not a finding about the code.
            matches=$(check_file "$file") || {
                echo -e "${RED}ERROR: this check failed to run (awk exited $?)${NC}"
                echo "  while checking: $file"
                echo "  This is a bug in scripts/check-anyhow-error-msg.sh, not a violation in that file."
                exit 1
            }
            if [ -n "$matches" ]; then
                echo -e "${RED}Found violation in $file:${NC}"
                while IFS=: read -r line_num line_content; do
                    echo -e "  Line $line_num: ${YELLOW}$line_content${NC}"
                done <<< "$matches"
                VIOLATIONS_FOUND=1
            fi
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
