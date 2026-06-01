#!/bin/bash
# Walk Driver Usage Linter
# Ensures the single-tree tools (copy, chmod, rm) traverse directories ONLY via
# the shared safe-walk driver (common/src/walk_driver.rs) and never hand-roll a
# recursive walk. A hand-rolled walk is detected by the presence of `JoinSet`
# (spawning child tasks) or `.read_entries(` (enumerating a directory) in the
# tool's own module.
#
# Why: the recursive spawn/classify/permit/drop-before-recurse skeleton — and in
# particular the leaf-permit "drop before recursion" deadlock invariant — lives in
# exactly one place (the driver). Reintroducing a JoinSet or read_entries in a tool
# would fork that skeleton and risk silently re-creating the hold-and-wait deadlock
# class the driver was built to prevent.
#
# Exemptions:
#   * common/src/link.rs        — rlink is the documented dual-tree special case
#                                 (two correlated trees) and keeps its own walk
#                                 while sharing the driver's substrate.
#   * common/src/walk_driver.rs — where the shared walk legitimately lives.
#
# This script uses only standard Unix tools (grep) available in GitHub CI.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Checking walk-driver usage (no hand-rolled walks in copy/chmod/rm)..."

VIOLATIONS_FOUND=0

# Files that must traverse via the shared driver (NOT link.rs / walk_driver.rs).
DRIVER_TOOLS="common/src/copy.rs common/src/chmod.rs common/src/rm.rs"

# Patterns that indicate a hand-rolled directory walk.
PATTERNS="JoinSet .read_entries("

for file in $DRIVER_TOOLS; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}ERROR: expected file not found: $file${NC}"
        exit 1
    fi
    for pattern in $PATTERNS; do
        # -F: fixed string, -n: line numbers; tolerate "no match" under `set -e`.
        matches=$(grep -Fn -- "$pattern" "$file" || true)
        if [ -n "$matches" ]; then
            echo -e "${RED}Found hand-rolled-walk marker '$pattern' in $file:${NC}"
            while IFS=: read -r line_num line_content; do
                echo -e "  Line $line_num: ${YELLOW}$line_content${NC}"
            done <<< "$matches"
            VIOLATIONS_FOUND=1
        fi
    done
done

if [ $VIOLATIONS_FOUND -eq 1 ]; then
    echo ""
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}ERROR: Found a hand-rolled directory walk in a driver-backed tool${NC}"
    echo -e "${RED}copy/chmod/rm must traverse via common/src/walk_driver.rs!${NC}"
    echo ""
    echo "These tools are WalkVisitor impls: implement the visitor hooks"
    echo "(visit_leaf / dir_pre / dir_post / on_skip / ...) and let the driver own"
    echo "the recursive spawn/classify/permit/drop-before-recurse skeleton."
    echo ""
    echo -e "Do NOT reintroduce ${RED}JoinSet${NC} or ${RED}.read_entries(${NC} here — that forks"
    echo "the walk and risks re-creating the leaf-permit hold-and-wait deadlock."
    echo ""
    echo "rlink (common/src/link.rs) is the documented dual-tree exemption."
    echo "See docs/tocttou.md and common/src/walk_driver.rs for details."
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Walk-driver usage check passed!${NC}"
exit 0
