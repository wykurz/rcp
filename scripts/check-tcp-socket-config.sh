#!/bin/bash
# TCP Socket Configuration Linter
# Ensures every rcp TCP connection is configured through the single funnel
# `remote::configure_tcp_socket` (remote/src/lib.rs). Two rules, because either one alone
# leaves a hole:
#
#   1. NO HAND-SET OPTION. TCP_NODELAY, keepalive, TCP_USER_TIMEOUT and the socket buffer
#      sizes may not be set outside the funnel's body.
#   2. NO UNCONFIGURED CONNECTION. Establishing or accepting a TCP connection is confined to
#      the funnel FILE's configured helpers, in two parts:
#        2a. Outside the funnel file, `TcpStream::connect(` / `.accept()` are forbidden
#            entirely — callers use the configured helpers (`connect_tcp_control`,
#            `connect_tcp_data`, `accept_tcp_control`, `accept_tcp_data`).
#        2b. Inside the funnel file, every top-level production `fn` that opens a connection
#            must configure THAT connection: each `let <ident> = ...connect/accept` needs a
#            `configure_tcp_socket(&<ident>...)` naming the same binding, and the fn needs at
#            least as many configure calls as opens — a helper opening two sockets but
#            configuring one, or configuring an unrelated stream, fails. `#[cfg(test)]` modules are skipped
#            (their sockets are throwaway fixtures, same rationale as the tls.rs exemption).
#            A connect/accept outside any recognized top-level fn — including inside an
#            `impl` block — fails CLOSED: the helpers are free functions precisely so this
#            scoping stays trivial to verify.
#      Rule 1 cannot see an unconfigured connection at all: it sets no option, so it passes
#      rule 1 while silently having no liveness detection. An earlier form of rule 2 compared
#      per-FILE counts of connect/accept sites vs `configure_tcp_socket(` mentions; in the
#      funnel file itself the declaration and its tests inflate that count, leaving room for
#      several unconfigured connections to land without failing CI.
#
# Why: these options used to be hand-paired at each connection — `set_nodelay` at 7 sites,
# buffer sizing at 4, and two sites got no buffer sizing at all. Adding keepalive and
# TCP_USER_TIMEOUT to every one of those sites is the missed-exit-path smell CLAUDE.md names:
# the next option is copied into six sites and forgotten in the seventh, and a connection with
# no liveness detection hangs forever on a peer whose host vanished. One funnel makes a
# reviewer verify the set once instead of seven times.
#
# Exemptions:
#   * the body of `configure_tcp_socket` itself (rule 1) — where the options are legitimately
#     set. The rest of the funnel file is NOT exempt from rule 1.
#   * remote/src/tls.rs (rule 2 only) — its connects/accepts are all inside #[cfg(test)]
#     handshake fixtures, which exercise TLS over a throwaway socket and carry no copy.
#
# The check also fails when a pattern is missing from the funnel entirely: without that, a
# rename or a deleted option would make the whole check pass vacuously.
#
# Lexing is line-based (comment stripping via `//.*`), not a full parser: a pattern inside a
# string literal or block comment can still count, and a `//` inside a string truncates the
# scan of that line. Sound for the shapes this codebase uses; the exact behaviour in both
# directions is pinned by scripts/test-check-tcp-socket-config.sh against the fixtures in
# scripts/tests/tcp-socket-config/ — add a case there whenever a rule changes. The test-mod
# skip in 2b recognizes `#[cfg(test)]` on the line immediately above a column-0 `mod`; a
# test fn placed bare in the funnel file is checked like production code (fail-closed).
#
# This script uses only standard Unix tools (grep, awk, sed) available in GitHub CI.
#
# Usage: check-tcp-socket-config.sh [search-dir...]
#   With no arguments, scans the repo's crate source directories. Arguments override the
#   search directories so scripts/test-check-tcp-socket-config.sh can point it at fixtures;
#   TCP_CHECK_FUNNEL_FILE and TCP_CHECK_CONNECT_EXEMPT override the funnel/exempt paths the
#   same way.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Checking that every TCP connection is configured through configure_tcp_socket..."

if [ "$#" -gt 0 ]; then
    SEARCH_DIRS="$*"
else
    SEARCH_DIRS="common/src rcp/src rlink/src rrm/src rcmp/src filegen/src remote/src"
fi
FUNNEL_FILE="${TCP_CHECK_FUNNEL_FILE:-remote/src/lib.rs}"
FUNNEL_FN="configure_tcp_socket"
# the socket options the funnel owns; getters (`nodelay()`, `send_buffer_size()`) are reads and
# do not match
PATTERNS="set_nodelay set_tcp_keepalive set_tcp_user_timeout set_send_buffer_size set_recv_buffer_size"
# rule 2: establishing or accepting a connection is confined to the funnel file's helpers
CONNECT_PATTERNS="TcpStream::connect( .accept()"
CONNECT_EXEMPT="${TCP_CHECK_CONNECT_EXEMPT:-remote/src/tls.rs}"

VIOLATIONS_FOUND=0

if [ ! -f "$FUNNEL_FILE" ]; then
    echo -e "${RED}ERROR: expected file not found: $FUNNEL_FILE${NC}"
    exit 1
fi

EXISTING_DIRS=""
for dir in $SEARCH_DIRS; do
    [ -d "$dir" ] && EXISTING_DIRS="$EXISTING_DIRS $dir"
done

# ── Rule 1 ──────────────────────────────────────────────────────────────────────────────────────
# Report every option-setter match outside the funnel's body, and every pattern the funnel no
# longer contains. The body runs from the `pub fn <name>(` line to the next `}` in column 0. Line
# comments are stripped first so prose naming an option — including this funnel's own doc
# comment — is fine.
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

# ── Rule 2a ─────────────────────────────────────────────────────────────────────────────────────
# Outside the funnel file (and the tls.rs exemption), raw connects/accepts are forbidden
# entirely. Line comments are stripped first, so prose naming a connect is fine.
while IFS= read -r file; do
    [ "$file" = "$CONNECT_EXEMPT" ] && continue
    [ "$file" = "$FUNNEL_FILE" ] && continue
    code=$(sed 's://.*::' "$file")
    all_hits=""
    for pattern in $CONNECT_PATTERNS; do
        hits=$(printf '%s\n' "$code" | grep -Fn -- "$pattern" || true)
        [ -z "$hits" ] && continue
        all_hits="${all_hits}${hits}"$'\n'
    done
    [ -z "$all_hits" ] && continue
    echo -e "${RED}Raw TCP connect/accept outside the funnel file in $file${NC} (use the remote:: connect/accept helpers):"
    while IFS=: read -r line_num line_content; do
        [ -z "$line_num" ] && continue
        echo -e "  Line $line_num: ${YELLOW}$line_content${NC}"
    done <<< "$all_hits"
    VIOLATIONS_FOUND=1
done < <(find $EXISTING_DIRS -name "*.rs" -type f | sort)

# ── Rule 2b ─────────────────────────────────────────────────────────────────────────────────────
# Inside the funnel file, every top-level production fn containing a connect/accept must call
# configure_tcp_socket( in its own body. #[cfg(test)] modules are skipped; a connect outside any
# recognized top-level fn fails closed as "stray".
report2=$(awk -v patterns="$CONNECT_PATTERNS" -v cfgfn="$FUNNEL_FN" '
BEGIN { n = split(patterns, pat, " ") }
{
    # test-module skipping: `#[cfg(test)]` immediately above a column-0 `mod`; the module ends
    # at the next `}` in column 0 (its items are indented)
    if (in_test) {
        if ($0 ~ /^}/) in_test = 0
        next
    }
    if (pending_test && $0 ~ /^(pub )?mod /) { in_test = 1; pending_test = 0; next }
    pending_test = ($0 ~ /^#\[cfg\(test\)\]/) ? 1 : 0
    code = $0
    sub(/\/\/.*/, "", code)
    # a new top-level fn: flush nothing (the previous one was flushed at its closing brace)
    if ($0 ~ /^(pub(\([a-z]+\))? )?(async )?(unsafe )?fn [A-Za-z_]/) {
        in_fn = 1
        opens = 0
        last_let = ""
        want_cfg_arg = 0
        delete open_line
        delete open_ident
        delete opens_by
        delete cfgs_by
    }
    # remember the most recent `let` binding: a multi-line open (`let stream = timeout(\n
    # TcpStream::connect(...)`) carries its pattern on a continuation line, which is attributed
    # to this binding
    if (in_fn && match(code, /let[ \t]+\(?[A-Za-z_][A-Za-z0-9_]*/)) {
        last_let = substr(code, RSTART, RLENGTH)
        sub(/let[ \t]+\(?/, "", last_let)
    }
    for (i = 1; i <= n; i++) {
        if (index(code, pat[i]) > 0) {
            if (!in_fn) { printf "stray:%d:%s\n", FNR, $0; continue }
            # per-SOCKET pairing: extract the binding identifier of the opened stream —
            # `let stream = ...connect(...)` / `let (stream, addr) = ...accept()` — and demand a
            # configure call naming that identifier. An open whose shape we cannot parse gets
            # ident "" and can only be satisfied by the count backstop failing it (fail closed).
            opens++
            open_line[opens] = FNR ":" $0
            ident = last_let
            open_ident[opens] = ident
            if (ident != "") opens_by[ident]++
        }
    }
    if (in_fn && want_cfg_arg) {
        # a multi-line configure call: its first argument is the leading `&ident` of this line
        want_cfg_arg = 0
        if (match(code, /^[ \t]*&?[A-Za-z_][A-Za-z0-9_]*/)) {
            arg = substr(code, RSTART, RLENGTH)
            gsub(/[ \t&]/, "", arg)
            cfgs_by[arg]++
        }
    }
    if (in_fn && index(code, cfgfn "(") > 0 && $0 !~ /^(pub )?fn /) {
        # record which identifier this configure names: the first argument, `&ident` or `ident`,
        # on this line or (rustfmt multi-line call) the next. counted PER IDENT so shadowed
        # same-name opens cannot share one configure.
        rest = substr(code, index(code, cfgfn "(") + length(cfgfn) + 1)
        if (match(rest, /^[ \t]*&?[A-Za-z_][A-Za-z0-9_]*/)) {
            arg = substr(rest, RSTART, RLENGTH)
            gsub(/[ \t&]/, "", arg)
            cfgs_by[arg]++
        } else if (rest ~ /^[ \t]*$/) {
            want_cfg_arg = 1
        }
    }
    if (in_fn && $0 ~ /^}/) {
        for (o = 1; o <= opens; o++) {
            # every open needs a configure naming ITS binding, at least as many times as that
            # binding is opened; an unparsable open shape fails closed
            id = open_ident[o]
            if (id == "" || opens_by[id] > cfgs_by[id])
                print "unconfigured:" open_line[o]
        }
        in_fn = 0
    }
}
END {
    # an unterminated fn at EOF still reports its hits (fail closed)
    if (in_fn)
        for (o = 1; o <= opens; o++) {
            id = open_ident[o]
            if (id == "" || opens_by[id] > cfgs_by[id])
                print "unconfigured:" open_line[o]
        }
}
' "$FUNNEL_FILE") || {
    echo -e "${RED}ERROR: this check failed to run (awk exited $?)${NC}"
    echo "  This is a bug in scripts/check-tcp-socket-config.sh, not a violation in the sources."
    exit 1
}

if [ -n "$report2" ]; then
    echo -e "${RED}Unconfigured TCP connection(s) inside $FUNNEL_FILE${NC} (each connecting/accepting fn must call $FUNNEL_FN on what it opened; 'stray' = outside any top-level fn):"
    while IFS=: read -r kind line_num content; do
        [ -z "$line_num" ] && continue
        echo -e "  Line $line_num: ${YELLOW}$content${NC}"
    done <<< "$report2"
    VIOLATIONS_FOUND=1
fi

if [ $VIOLATIONS_FOUND -eq 1 ]; then
    echo ""
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}ERROR: TCP socket options must be set in ONE place${NC}"
    echo -e "${RED}A connection configured by hand will miss an option — and a connection${NC}"
    echo -e "${RED}without keepalive/TCP_USER_TIMEOUT hangs forever on a vanished host.${NC}"
    echo ""
    echo "Instead of setting an option by hand — or of opening a connection directly:"
    echo -e "  ${RED}stream.set_nodelay(true)?;   TcpStream::connect(addr)   listener.accept()${NC}"
    echo ""
    echo "Establish or accept through the configured helpers in remote/src/lib.rs:"
    echo -e "  ${GREEN}remote::connect_tcp_control(addr, tcp_config)  /  remote::connect_tcp_data(addr, profile, keepalive)${NC}"
    echo -e "  ${GREEN}remote::accept_tcp_control(&listener, tcp_config)  /  remote::accept_tcp_data(&listener, profile, keepalive)${NC}"
    echo -e "  ${GREEN}(a NEW helper belongs beside them, calling configure_tcp_socket on what it opens)${NC}"
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Every TCP connection is configured through $FUNNEL_FN!${NC}"
exit 0
