#!/bin/bash
# Error Logging Format Linter
# Ensures anyhow::Error types are logged with {:#} or {:?} to preserve error chains
#
# This script uses only standard Unix tools (grep, sed, awk) available in GitHub CI

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Checking error logging format..."

VIOLATIONS_FOUND=0
TEMP_FILE=$(mktemp)
trap 'rm -f "$TEMP_FILE"' EXIT

# Directories to check
SEARCH_DIRS="common/src rcp/src rlink/src rrm/src rcmp/src filegen/src"

# Function to check a single file for violations
check_file() {
    local file="$1"
    local violations=0

    # Read the file and check for problematic patterns
    # We need to handle both single-line and multi-line tracing::error! calls

    # Use awk to handle multi-line tracing::error! statements
    awk '
    BEGIN {
        in_error_call = 0
        error_start_line = 0
        error_text = ""
    }

    # Skip doc comments and regular comments
    /^[[:space:]]*\/\/[\/!]/ { next }
    /^[[:space:]]*\/\*/ { next }

    # Detect start of tracing::error! call
    /tracing::error!/ {
        in_error_call = 1
        error_start_line = NR
        error_text = $0

        # Check if this is a single-line call (ends with );)
        if (/\);/) {
            in_error_call = 0
            # Check for violation: has {} format (not {:#} or {:?}) and has error variable
            if (error_text ~ /\{\}"/ && error_text !~ /{:#}"/ && error_text !~ /{:\?}"/ && (error_text ~ /&error\)/ || error_text ~ /, e\)/)) {
                print error_start_line ":" error_text
            }
            error_text = ""
        }
        next
    }

    # Continue collecting multi-line error call
    in_error_call {
        error_text = error_text " " $0

        # Check if this line ends the call
        if (/\);/ || /^[[:space:]]*\);/) {
            in_error_call = 0
            # Check for violation
            if (error_text ~ /\{\}"/ && error_text !~ /{:#}"/ && error_text !~ /{:\?}"/ && (error_text ~ /&error/ || error_text ~ /, e\)/)) {
                print error_start_line ":" error_text
            }
            error_text = ""
        }
    }
    ' "$file" > "$TEMP_FILE"

    if [ -s "$TEMP_FILE" ]; then
        echo -e "${RED}Found violations in $file:${NC}"
        while IFS=: read -r line_num content; do
            # Clean up the content for display
            cleaned=$(echo "$content" | sed 's/  */ /g' | sed 's/^ //')
            echo -e "  Line $line_num: ${YELLOW}$cleaned${NC}"
        done < "$TEMP_FILE"
        return 1
    fi

    return 0
}

# Check all Rust files in the specified directories
for dir in $SEARCH_DIRS; do
    if [ -d "$dir" ]; then
        while IFS= read -r file; do
            if ! check_file "$file"; then
                VIOLATIONS_FOUND=1
            fi
        done < <(find "$dir" -name "*.rs" -type f)
    fi
done

if [ $VIOLATIONS_FOUND -eq 1 ]; then
    echo ""
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}ERROR: Found error logs using Display format {} instead of {:#} or {:?}${NC}"
    echo -e "${RED}This loses error chain information!${NC}"
    echo ""
    echo "Fix by changing:"
    echo -e "  ${RED}tracing::error!(\"...: {}\", &error);${NC}"
    echo "To either:"
    echo -e "  ${GREEN}tracing::error!(\"...: {:#}\", &error);  // Compact, inline${NC}"
    echo -e "  ${GREEN}tracing::error!(\"...: {:?}\", &error);  // Detailed, multi-line${NC}"
    echo ""
    echo "See CLAUDE.md for details."
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Error logging format check passed!${NC}"
exit 0
