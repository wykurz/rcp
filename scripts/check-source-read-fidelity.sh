#!/bin/bash
# Source-Read Fidelity Linter
# Forbids reading a SOURCE object's payload by name/path in the hardened read modules — the vector
# that desyncs payload from metadata (a same-name swap pairing one inode's bytes/target with another
# inode's metadata). Source payload reads must go through the fd-paired primitives, so payload and
# metadata come from the SAME fd:
#   files    -> Dir::open_file_read(name) -> (File, FileMeta)
#   symlinks -> Handle::read_symlink(side) -> (PathBuf, FileMeta)
#   dirs     -> Dir::entries/read_entries + Dir::meta (same held directory)
#
# Legitimate exceptions — the `-L`/--dereference path-based walk (intentionally not hardened) and
# destination-side reads — are marked inline with `// rcp-toctou-allow: <reason>` and skipped.
#
# Scope: lines before the final top-level #[cfg(test)] module. Test-only fields and hooks do not
# end the scan; each scanned file keeps its unit-test module at the bottom, or has none.
#
# Uses only standard Unix tools available in GitHub CI.

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
echo "🔍 Checking source-read fidelity (no by-name/path source payload reads)..."

# safedir.rs is scanned too because it is where the fd-paired primitives themselves live: a by-path
# read added next to them is exactly the regression the `*xattr` patterns below exist to catch, and
# it would not be caught by scanning only the callers. Its own tests read by path deliberately and
# sit inside the final test module, which this scan excludes.
FILES="common/src/copy.rs common/src/link.rs common/src/safedir.rs rcp/src/source.rs rcp/src/source/discovery.rs"
# the by-name / by-path SOURCE payload reads. NOT metadata/symlink_metadata: those have legitimate
# dst-existence / -L / test uses and are not the drift vector (metadata pairing is structural).
#
# The `*xattr` entries cover POSIX ACL reads, which are a source payload like any other: an ACL read
# by PATH can be answered by a different inode than the one whose bytes and metadata were read from
# the held fd, pairing one entry's permissions with another's contents. The fd forms (`fgetxattr` /
# `flistxattr`, used by `safedir::read_acls_fd`) are the correct ones and are deliberately absent
# here. The `l`-prefixed forms are listed too: they do not follow a final symlink, but they still
# resolve the name. The path-based directory cursor is reserved for marked -L callers; match its
# associated call syntax rather than its definition in safedir.rs.
PATTERNS=".read_link_at( tokio::fs::read_link( tokio::fs::File::open( std::fs::File::open( ::open_following_symlinks( libc::getxattr( libc::lgetxattr( libc::listxattr( libc::llistxattr("
MARKER="rcp-toctou-allow:"
VIOLATIONS=0

for file in $FILES; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}ERROR: expected file not found: $file${NC}"
        exit 1
    fi
    # only a top-level test module ends production code; a cfg(test) field or hook cannot hide it.
    end=$(awk '
        /^#\[cfg\(test\)\]$/ { test_attribute = NR; next }
        /^mod [[:alnum:]_]+[[:space:]]*\{/ && test_attribute == NR - 1 {
            print test_attribute - 1; found = 1; exit
        }
        END { if (!found) print NR }
    ' "$file")
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
