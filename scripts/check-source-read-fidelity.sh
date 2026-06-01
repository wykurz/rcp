#!/bin/bash
# Source-Read Fidelity Linter
# Forbids reading a SOURCE object's payload by name/path in the hardened read modules — the vector
# that desyncs payload from metadata (a same-name swap pairing one inode's bytes/target with another
# inode's metadata). Source payload reads must go through the fd-paired primitives, so payload and
# metadata come from the SAME fd:
#   files    -> Dir::open_file_read(name) -> (File, FileMeta)
#   symlinks -> Handle::read_symlink(side) -> (PathBuf, FileMeta)
#   dirs     -> Dir::read_entries + Dir::meta (same fd)
#
# Legitimate exceptions — the `-L`/--dereference path-based walk (intentionally not hardened) and
# destination-side reads — are marked inline with `// rcp-toctou-allow: <reason>` and skipped.
#
# Scope: non-test code only (lines before the first `#[cfg(test)]`; each scanned file keeps its unit
# tests in a single module at the bottom, or has none).
#
# Uses only standard Unix tools (grep/head/cut) available in GitHub CI.

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
echo "🔍 Checking source-read fidelity (no by-name/path source payload reads)..."

FILES="common/src/copy.rs common/src/link.rs rcp/src/source.rs"
# the by-name / by-path SOURCE payload reads. NOT metadata/symlink_metadata: those have legitimate
# dst-existence / -L / test uses and are not the drift vector (metadata pairing is structural).
PATTERNS=".read_link_at( tokio::fs::read_link( tokio::fs::File::open( std::fs::File::open("
MARKER="rcp-toctou-allow:"
VIOLATIONS=0

for file in $FILES; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}ERROR: expected file not found: $file${NC}"
        exit 1
    fi
    # non-test portion: lines before the first `#[cfg(test)]` (whole file if none).
    cut_line=$(grep -n -m1 -F '#[cfg(test)]' "$file" | cut -d: -f1 || true)
    if [ -n "$cut_line" ]; then
        end=$((cut_line - 1))
    else
        end=$(wc -l < "$file")
    fi
    body=$(head -n "$end" "$file")
    for pattern in $PATTERNS; do
        # -F fixed string, -n line numbers; drop allow-marked lines; tolerate no-match under set -e.
        hits=$(printf '%s\n' "$body" | grep -Fn -- "$pattern" | grep -vF "$MARKER" || true)
        if [ -n "$hits" ]; then
            echo -e "${RED}Unmarked source-payload-by-name read '$pattern' in $file:${NC}"
            while IFS=: read -r n c; do
                echo -e "  Line $n: ${YELLOW}$c${NC}"
            done <<< "$hits"
            VIOLATIONS=1
        fi
    done
done

if [ $VIOLATIONS -eq 1 ]; then
    echo ""
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}ERROR: a source object's payload is read by name/path in a hardened module${NC}"
    echo ""
    echo "Read source payloads through the fd-paired primitives so payload and metadata come from"
    echo "the SAME fd: open_file_read (files), Handle::read_symlink (symlinks), read_entries +"
    echo "Dir::meta (dirs). Otherwise a same-name swap can pair one inode's bytes/target with"
    echo "another inode's metadata (a fidelity drift)."
    echo ""
    echo "If this read is genuinely safe (the -L/--dereference path, or a destination-side read),"
    echo -e "append ${YELLOW}// rcp-toctou-allow: <reason>${NC} to the line."
    echo "See docs/tocttou.md."
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Source-read fidelity check passed!${NC}"
exit 0
