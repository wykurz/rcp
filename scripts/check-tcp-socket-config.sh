#!/bin/bash
# TCP Socket Configuration Linter
# Ensures every rcp TCP connection is configured through the single funnel
# `remote::configure_tcp_socket` (remote/src/lib.rs). Two rules, because either one alone
# leaves a hole:
#
#   1. NO HAND-SET OPTION. TCP_NODELAY, keepalive, TCP_USER_TIMEOUT and the socket buffer
#      sizes may not be set outside the funnel's body.
#   2. NO UNCONFIGURED CONNECTION. A file that opens (`TcpStream::connect(`) or accepts
#      (`.accept()`) a TCP connection must also call `configure_tcp_socket`. Rule 1 cannot
#      see this case at all: a new connection that nobody configures sets no option, so it
#      passes rule 1 while silently having no liveness detection.
#
# Why: these options used to be hand-paired at each connection — `set_nodelay` at 7 sites,
# buffer sizing at 4, and two sites got no buffer sizing at all. Adding keepalive and
# TCP_USER_TIMEOUT to every one of those sites is the missed-exit-path smell CLAUDE.md names:
# the next option is copied into six sites and forgotten in the seventh, and a connection with
# no liveness detection hangs forever on a peer whose host vanished. One funnel makes a
# reviewer verify the set once instead of seven times.
#
# Exemptions:
#   * the body of `configure_tcp_socket` itself — where the options are legitimately set. The
#     rest of remote/src/lib.rs is NOT exempt, so a new connection helper added beside it is
#     still caught.
#   * remote/src/tls.rs (rule 2 only) — its connects/accepts are all inside #[cfg(test)]
#     handshake fixtures, which exercise TLS over a throwaway socket and carry no copy.
#
# The check also fails when a pattern is missing from the funnel entirely: without that, a
# rename or a deleted option would make the whole check pass vacuously.
#
# This script uses only standard Unix tools (grep, awk) available in GitHub CI.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Checking that every TCP connection is configured through configure_tcp_socket..."

SEARCH_DIRS="common/src rcp/src rlink/src rrm/src rcmp/src filegen/src remote/src"
FUNNEL_FILE="remote/src/lib.rs"
FUNNEL_FN="configure_tcp_socket"
# the socket options the funnel owns; getters (`nodelay()`, `send_buffer_size()`) are reads and
# do not match
PATTERNS="set_nodelay set_tcp_keepalive set_tcp_user_timeout set_send_buffer_size set_recv_buffer_size"
# rule 2: establishing or accepting a connection obliges a file to configure it
CONNECT_PATTERNS="TcpStream::connect( .accept()"
CONNECT_EXEMPT="remote/src/tls.rs"

VIOLATIONS_FOUND=0

if [ ! -f "$FUNNEL_FILE" ]; then
    echo -e "${RED}ERROR: expected file not found: $FUNNEL_FILE${NC}"
    exit 1
fi

EXISTING_DIRS=""
for dir in $SEARCH_DIRS; do
    [ -d "$dir" ] && EXISTING_DIRS="$EXISTING_DIRS $dir"
done

# Report every match outside the funnel's body, and every pattern the funnel no longer contains.
# The body runs from the `pub fn <name>(` line to the next `}` in column 0. Line comments are
# stripped first so prose naming an option — including this funnel's own doc comment — is fine.
report=$(awk -v fn="$FUNNEL_FN" -v patterns="$PATTERNS" '
BEGIN { n = split(patterns, pat, " ") }
{
    if ($0 ~ ("^pub fn " fn "\\(")) inside = 1
    code = $0
    sub(/\/\/.*/, "", code)
    for (i = 1; i <= n; i++) {
        if (index(code, pat[i]) > 0) {
            if (inside) seen[pat[i]]++
            else printf "outside:%d:%s:%s\n", FNR, FILENAME, $0
        }
    }
    if (inside && $0 ~ /^}/) inside = 0
}
END {
    for (i = 1; i <= n; i++)
        if (!(pat[i] in seen)) printf "missing:0::%s\n", pat[i]
}
' $(find $EXISTING_DIRS -name "*.rs" -type f | sort)) || {
    # awk's own failure — a syntax error in the program above, an unreadable file — produces empty
    # output, which is indistinguishable from a clean scan, so the check would report success for a
    # scan that never ran. Fatal rather than counted as a violation: a linter that did not run is not a
    # finding about the code.
    echo -e "${RED}ERROR: this check failed to run (awk exited $?)${NC}"
    echo "  This is a bug in scripts/check-tcp-socket-config.sh, not a violation in the sources."
    exit 1
}

if [ -n "$report" ]; then
    while IFS=: read -r kind line_num file content; do
        if [ "$kind" = "missing" ]; then
            echo -e "${RED}Funnel $FUNNEL_FILE::$FUNNEL_FN no longer sets:${NC} ${YELLOW}$content${NC}"
        else
            echo -e "${RED}Socket option set outside $FUNNEL_FN in $file:${NC}"
            echo -e "  Line $line_num: ${YELLOW}$content${NC}"
        fi
    done <<< "$report"
    VIOLATIONS_FOUND=1
fi

# Rule 2: a file that establishes or accepts a connection must also configure it. Line comments
# are stripped first, as above, so prose naming a connect is fine.
while IFS= read -r file; do
    [ "$file" = "$CONNECT_EXEMPT" ] && continue
    code=$(sed 's://.*::' "$file")
    for pattern in $CONNECT_PATTERNS; do
        hits=$(printf '%s\n' "$code" | grep -Fn -- "$pattern" || true)
        [ -z "$hits" ] && continue
        # bash pattern match rather than `grep -q`: under `pipefail` a `grep -q` that exits on its
        # first match SIGPIPEs the producer, and the pipeline reports failure despite the match
        if [[ "$code" != *"$FUNNEL_FN"* ]]; then
            echo -e "${RED}Unconfigured TCP connection in $file:${NC}"
            while IFS=: read -r line_num line_content; do
                echo -e "  Line $line_num: ${YELLOW}$line_content${NC}"
            done <<< "$hits"
            VIOLATIONS_FOUND=1
            break
        fi
    done
done < <(find $EXISTING_DIRS -name "*.rs" -type f | sort)

if [ $VIOLATIONS_FOUND -eq 1 ]; then
    echo ""
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}ERROR: TCP socket options must be set in ONE place${NC}"
    echo -e "${RED}A connection configured by hand will miss an option — and a connection${NC}"
    echo -e "${RED}without keepalive/TCP_USER_TIMEOUT hangs forever on a vanished host.${NC}"
    echo ""
    echo "Instead of setting an option by hand — or of leaving a connection untouched:"
    echo -e "  ${RED}stream.set_nodelay(true)?;${NC}"
    echo ""
    echo "Configure the connection where it is established or accepted:"
    echo -e "  ${GREEN}remote::configure_tcp_socket(&stream, network_profile, keepalive_sec, kind);${NC}"
    echo -e "  ${GREEN}kind is ConnectionKind::Control or ::Data (data connections get no user timeout)${NC}"
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Every TCP connection is configured through $FUNNEL_FN!${NC}"
exit 0
