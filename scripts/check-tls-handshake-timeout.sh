#!/bin/bash
# TLS Handshake Timeout Linter
# Ensures every production TLS handshake goes through the bounded helpers in
# remote/src/tls.rs (`accept_bounded` / `connect_bounded`) and never calls
# `TlsAcceptor::accept` / `TlsConnector::connect` directly.
#
# Why: the handshake runs AFTER the TCP accept/connect is already bounded, so a peer
# that completes TCP and then sends no TLS bytes stalls the future forever. On rcpd's
# listener — whose accepts are sequential with the handshake inline — one such peer
# blocks the legitimate master from ever connecting. That timeout was added at three
# call sites and forgotten at two; a single funnel makes forgetting it impossible.
#
# Exemption:
#   * remote/src/tls.rs — where the bounded helpers (and their own tests) live.
#
# This script uses only standard Unix tools (grep) available in GitHub CI.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Checking that every TLS handshake is bounded..."

SEARCH_DIRS="common/src rcp/src rlink/src rrm/src rcmp/src filegen/src remote/src"
EXEMPT="remote/src/tls.rs"

# Matches `.accept(`/`.connect(` only when the receiver is named `acceptor`/`connector` — the names
# the TLS types are always bound to here. Plain `TcpListener::accept` and `TcpStream::connect`
# therefore do not trip it.
#
# rustfmt splits these calls across lines:
#
#     let tls_stream = acceptor
#         .accept(stream)
#
# so a line-oriented grep would never see the receiver and the method together — it would miss the
# exact shape of the original bug. Continuation lines starting with `.` are folded onto the previous
# line first (reporting that line's number), so the whole call chain is matched as one unit.
fold_continuations() {
    awk '
    {
        line = $0
        sub(/^[[:space:]]+/, "", line)
        if (line ~ /^\./ && held != "") {
            held = held line
            next
        }
        if (held != "") print heldno ":" held
        held = line
        heldno = NR
    }
    END { if (held != "") print heldno ":" held }
    ' "$1"
}

VIOLATIONS_FOUND=0

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

for dir in $SEARCH_DIRS; do
    [ -d "$dir" ] || continue
    while IFS= read -r file; do
        [ "$file" = "$EXEMPT" ] && continue
        # strip line comments before matching so prose mentioning the call is fine, then fold
        # rustfmt's multi-line call chains so the receiver and the method are seen together.
        #
        # The fold runs on its own rather than inside the pipeline below, so that awk's own failure — a
        # syntax error in the program above, an unreadable file — is seen. Piped, its status is
        # discarded, and its empty output is indistinguishable from a clean file: the check would report
        # success for a scan that never ran. Fatal rather than counted as a violation, because a linter
        # that did not run is not a finding about the code.
        sed 's://.*::' "$file" > "$tmp"
        folded=$(fold_continuations "$tmp") || {
            echo -e "${RED}ERROR: this check failed to run (awk exited $?)${NC}"
            echo "  while checking: $file"
            echo "  This is a bug in scripts/check-tls-handshake-timeout.sh, not a violation in that file."
            exit 1
        }
        matches=$(printf '%s\n' "$folded" \
            | grep -E '\.(accept|connect)[[:space:]]*\(' \
            | grep -E '(acceptor|connector)' || true)
        if [ -n "$matches" ]; then
            echo -e "${RED}Found violation in $file:${NC}"
            while IFS=: read -r line_num line_content; do
                echo -e "  Line $line_num: ${YELLOW}$line_content${NC}"
            done <<< "$matches"
            VIOLATIONS_FOUND=1
        fi
    done < <(find "$dir" -name "*.rs" -type f)
done

if [ $VIOLATIONS_FOUND -eq 1 ]; then
    echo ""
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}ERROR: Found an UNBOUNDED TLS handshake${NC}"
    echo -e "${RED}A peer that completes TCP then stalls would hang this path.${NC}"
    echo ""
    echo "Instead of:"
    echo -e "  ${RED}acceptor.accept(stream).await?${NC}"
    echo ""
    echo "Use the bounded helper (it also splits the stream and handles the non-TLS case):"
    echo -e "  ${GREEN}remote::tls::accept_bounded(acceptor, stream, timeout, \"control\").await?${NC}"
    echo -e "  ${GREEN}remote::tls::connect_bounded(connector, name, stream, timeout, \"data\").await?${NC}"
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Every TLS handshake is bounded!${NC}"
exit 0
