#!/bin/bash
# Markdown Format Check
# Ensures markdown docs are formatted with dprint (prose wrapped at 100 columns).
#
# Formatting output is fully determined by the plugin version + checksum pinned
# in dprint.json, so any recent dprint CLI produces identical results. The nix
# devshell provides dprint; CI installs a checksum-verified release binary.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo "🔍 Checking markdown formatting..."

if ! command -v dprint > /dev/null 2>&1; then
    echo -e "${RED}ERROR: dprint not found.${NC}"
    echo "It is included in the nix devshell (nix develop); otherwise install it"
    echo "from your package manager or https://dprint.dev"
    exit 1
fi

if ! dprint check; then
    echo ""
    echo -e "${RED}ERROR: Markdown files are not formatted.${NC}"
    echo -e "Fix with: ${GREEN}just fmt${NC} (or 'dprint fmt')"
    exit 1
fi

echo -e "${GREEN}✅ Markdown formatting check passed!${NC}"
exit 0
