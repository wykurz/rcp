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

# Directories to check. Overridable by argument so scripts/test-check-error-logging.sh can point
# the very same detector at its fixtures instead of re-implementing (and drifting from) the rules.
#
# An ARRAY, not a string: a string would be word-split on whitespace below, so a directory whose path
# contains a space silently became two nonexistent paths and was skipped without a word — the check
# then "passed" having scanned nothing.
if [ "$#" -gt 0 ]; then
    SEARCH_DIRS=("$@")
else
    SEARCH_DIRS=(
        common/src rcp/src rlink/src rrm/src rcmp/src rchm/src
        filegen/src remote/src throttle/src congestion/src
    )
fi

# Function to check a single file for violations
check_file() {
    local file="$1"
    local violations=0

    # Read the file and check for problematic patterns
    # We need to handle both single-line and multi-line tracing::(error|warn|info|debug|trace)! calls

    # Use awk to handle multi-line tracing::(error|warn|info|debug|trace)! statements
    awk '
    # Three ways an error can lose its chain in a log line, all checked by check() below:
    #   A — passed POSITIONALLY against a bare `{}`
    #   B — captured INLINE in the format string with no spec (`{e}`, `{my_error}`)
    #   C — recorded as a structured field with the Display sigil (`%error`)
    #
    # All apply to EVERY tracing level: an error logged at any level drops the same chain, and the
    # level says nothing about whether the chain matters — a test that logs its failure at info!
    # loses exactly as much diagnostic detail as production code logging at error!.
    #
    # THIS IS A HEURISTIC BACKSTOP, NOT A PROOF. It is awk, not a Rust parser: it cannot pair
    # arguments with placeholders in general, and it recognises errors by NAME. Both rules below are
    # written to be sound rather than complete — a violation it cannot decide is one it stays quiet
    # about — so a clean run means "none of the known shapes", not "no chain is dropped anywhere".
    # Its exact behaviour, in both directions, is pinned by scripts/test-check-error-logging.sh.
    #
    # A tracing call has two parts that must be scanned SEPARATELY, because a token means different
    # things in each: placeholders live only in the FORMAT STRING, error names only in the ARGUMENT
    # LIST. Scanning the raw line for both was unsound in both directions — a `{:#}` written in a
    # trailing comment counted as the call'\''s last placeholder and silenced a real bare `{}`, while an
    # `Err(error)` in a comment, in prose inside the format string, or in the match arm to the LEFT of
    # the macro counted as an error argument and invented one. So the helpers below do a small amount of
    # real LEXING first — comments out, call located, parens counted, format string and argument list
    # separated — and every rule then reads only the part its token can legally appear in. Each is a
    # replacement for a regex that guessed at the same thing and got it wrong in one direction or both.

    # Strip COMMENTS of BOTH forms in ONE left-to-right pass, so whichever delimiter comes first wins.
    # A separate pass per form gets it wrong in both orders: `// … /* …` opens a block comment that was
    # never there, and `/* … // … */` loses the terminator that closes it. String-aware, so
    # `"ssh://{}: {:#}"` and `"/*"` survive — a blind `sub(/\/\/.*/, "")` would truncate any format
    # string containing `//`.
    #
    # An unterminated `/*` sets `in_block_comment`, and the caller drops following lines until it
    # closes. Without that, only the FIRST line of a block comment was hidden, so a commented-out call
    # inside one was reported as a live violation — the linter rejecting valid Rust.
    function strip_comments(line,   out, i, n, c, j) {
        n = length(line)
        out = ""
        for (i = 1; i <= n; i++) {
            c = substr(line, i, 1)
            if (c == "\"") {
                # copy the whole literal through verbatim, honoring `\"`
                out = out c
                for (i++; i <= n; i++) {
                    c = substr(line, i, 1)
                    out = out c
                    if (c == "\\") { out = out substr(line, i + 1, 1); i++; continue }
                    if (c == "\"") { break }
                }
                continue
            }
            if (c == "/" && substr(line, i + 1, 1) == "/") { return out }
            if (c == "/" && substr(line, i + 1, 1) == "*") {
                j = index(substr(line, i + 2), "*/")
                if (j == 0) { in_block_comment = 1; return out }
                i = i + 2 + j
                continue
            }
            out = out c
        }
        return out
    }

    # Everything from the macro name rightwards. What precedes it belongs to the surrounding
    # expression, not to the call: a match arm'\''s `Err(error) =>` binding is not an argument, its
    # parens are not the call'\''s parens, and a string arm pattern (`"error" => tracing::…`) is not the
    # format string.
    function after_macro(text) {
        sub(/^.*tracing::(error|warn|info|debug|trace)!/, "", text)
        return text
    }

    # Net paren depth change outside string literals. This is what decides where a call ENDS, replacing
    # three regex guesses at the terminator (`);`, `),`, a lone `)`): a call ends exactly when its own
    # parens balance. The regexes could not see nesting, so an argument containing `);` — a nested call
    # in a block expression — cut collection short and hid whatever came after, including the error.
    function paren_delta(code,   i, n, c, d) {
        n = length(code)
        d = 0
        for (i = 1; i <= n; i++) {
            c = substr(code, i, 1)
            if (c == "\"") {
                for (i++; i <= n; i++) {
                    c = substr(code, i, 1)
                    if (c == "\\") { i++; continue }
                    if (c == "\"") { break }
                }
                continue
            }
            if (c == "(") { d++ } else if (c == ")") { d-- }
        }
        return d
    }

    # The FORMAT STRING: the first string literal of the call, with `\"` unescaped. Placeholders can
    # only occur here.
    #
    # Known gap, in the sound direction: a structured field whose VALUE is a literal
    # (`tracing::info!(path = "/tmp", "copied {}", n)`) makes this return the wrong literal, which
    # yields no placeholders and so no report — quiet, never a false alarm.
    function format_string(text,   i, n, c, out, started) {
        text = after_macro(text)
        n = length(text)
        started = 0
        out = ""
        for (i = 1; i <= n; i++) {
            c = substr(text, i, 1)
            if (!started) { if (c == "\"") { started = 1 }; continue }
            if (c == "\\") { out = out substr(text, i + 1, 1); i++; continue }
            if (c == "\"") { break }
            out = out c
        }
        return out
    }

    # The ARGUMENT LIST: the call from the macro name rightwards, with every string literal blanked
    # out. Name-based rules read this. Blanking the literals is what keeps prose inside the format
    # string ("no error, just {} files") from being read as an error ARGUMENT.
    function code_args(text,   i, n, c, out, in_str) {
        text = after_macro(text)
        n = length(text)
        in_str = 0
        out = ""
        for (i = 1; i <= n; i++) {
            c = substr(text, i, 1)
            if (in_str) {
                if (c == "\\") { i++; continue }
                if (c == "\"") { in_str = 0 }
                continue
            }
            if (c == "\"") { in_str = 1; continue }
            out = out c
        }
        return out
    }

    # `{{` and `}}` are Rust'\''s ESCAPES for literal braces, not placeholders. Strip them before any
    # placeholder scan, exactly as the compiler does, or `"map {{}} here", e` reads as a bare `{}`.
    function strip_brace_escapes(text) {
        gsub(/\{\{/, "", text)
        gsub(/\}\}/, "", text)
        return text
    }

    # The LAST placeholder, and whether EVERY placeholder is a bare `{}`. Together these give two
    # sound tests without needing to know which argument goes with which placeholder — which awk
    # cannot determine, since doing so means parsing Rust expressions.
    function last_placeholder(text,   rest, spec) {
        spec = ""
        rest = strip_brace_escapes(text)
        while (match(rest, /\{[^{}]*\}/)) {
            spec = substr(rest, RSTART, RLENGTH)
            rest = substr(rest, RSTART + RLENGTH)
        }
        return spec
    }

    function all_placeholders_bare(text,   rest, spec, total, bare) {
        total = 0; bare = 0
        rest = strip_brace_escapes(text)
        while (match(rest, /\{[^{}]*\}/)) {
            spec = substr(rest, RSTART, RLENGTH)
            total++
            if (spec == "{}") bare++
            rest = substr(rest, RSTART + RLENGTH)
        }
        return (total > 0 && total == bare)
    }

    # An error recognised by NAME: `e`, `err`, `error`, or anything ending in `_err` / `_error`
    # (join_err, write_error, combined_error), borrowed or not. The `_` boundary matters — matching
    # `err` anywhere would swallow `stderr`, `host_stderr` and `errored`, none of which are errors.
    #
    # `last_arg_is_error` anchors on the closing paren (with rustfmt'\''s optional trailing comma), so
    # it means the error is the FINAL argument. `has_error_arg` accepts one anywhere in the list, and
    # also accepts `}` after the name so an error that is the TAIL EXPRESSION of a block argument
    # (`…, { observe(); &error })`) still counts — it is followed by a brace rather than a comma.
    function last_arg_is_error(text) {
        return text ~ /, *&?(e|err|error|[a-z_]*_(err|error)) *,? *\)/
    }

    function has_error_arg(text) {
        return text ~ /[ ,(]&?(e|err|error|[a-z_]*_(err|error)) *[,)}]/
    }

    function check(text, line,   fmt, args) {
        # split the call into the two parts the rules read, so no token is judged by a rule that does
        # not govern where it appears (see the helpers above).
        fmt = format_string(text)
        args = code_args(text)

        # An error passed as a POSITIONAL argument. Two sound rules, neither of which needs to pair
        # arguments with placeholders:
        #   A1 — the LAST placeholder is bare AND the LAST argument is an error. This is the
        #        codebase convention (the error goes last), so it is the common case.
        #   A2 — EVERY placeholder is bare AND SOME argument is an error. Whichever position the
        #        error occupies, it is rendered with `{}`.
        # The two argument tests differ and must stay distinct: A1 anchors on the closing paren, so
        # `"{} … {:#}", name, &error` (error last, correctly formatted) stays clean, while A2 accepts
        # an error anywhere, which is what catches `"{} … {}", &error, dst`.
        #
        # Known gap, deliberately not closed: a MIXED format string whose error argument is NOT last
        # (`"{:#} … {}", &error, dst`). Deciding that needs a Rust expression parser, which this is
        # not, so the rules stay quiet rather than guess. Pinned in the clean fixture.
        if ((last_placeholder(fmt) == "{}" && last_arg_is_error(args)) ||
            (all_placeholders_bare(fmt) && has_error_arg(args))) {
            print line ":" text; return
        }

        # An error captured INLINE in the format string with no spec — `{e}`, `{err}`, `{my_error}`.
        # Brace escapes must go first, exactly as for the placeholder scans: `{{error}}` is the LITERAL
        # text `{error}`, not a capture, and reading it raw flagged a line that interpolates nothing.
        if (strip_brace_escapes(fmt) ~ /\{(e|err|error|[a-z_]*_(err|error))\}/) {
            print line ":" text; return
        }

        # tracing'\''s structured-field sigils: `%x` records x'\''s Display, `?x` its Debug. `%` on an
        # error is the same chain-dropping mistake as a bare `{}`; `?` is fine.
        if (args ~ /[ ,(]%(e|err|error|[a-z_]*_(err|error))[ ,)]/) { print line ":" text }
    }

    BEGIN {
        in_error_call = 0
        error_start_line = 0
        error_text = ""
        depth = 0
        armed = 0
        allowed = 0
        in_block_comment = 0
    }

    # ONE main block, rather than a rule per line shape: every line needs its comments removed before
    # anything can classify it, and that has to happen in exactly one place.
    {
        line = $0
        # inside a block comment, nothing is code until it closes
        if (in_block_comment) {
            close_at = index(line, "*/")
            if (close_at == 0) { next }
            in_block_comment = 0
            line = substr(line, close_at + 2)
        }
        code = strip_comments(line)

        # A line with no code left is a comment or a blank. It cannot be part of a call, but it CAN arm
        # the allow marker for the call below it — and that is the only placement AGENTS.md documents:
        # a justified comment on the line immediately above. Anything else must not silence a call.
        if (code ~ /^[[:space:]]*$/) {
            # A marker only arms BETWEEN calls. One that strayed INSIDE a multi-line call (where the
            # contract says it belongs above the macro) must not silence that call, and must not leak
            # forward to silence the next one either.
            if (!in_error_call) { armed = ($0 ~ /rcp-error-log-allow/) }
            next
        }

        if (in_error_call) {
            error_text = error_text " " code
            depth += paren_delta(code)
            if (depth <= 0) {
                in_error_call = 0
                if (!allowed) { check(error_text, error_start_line) }
                error_text = ""
            }
            next
        }

        if (code ~ /tracing::(error|warn|info|debug|trace)!/) {
            error_start_line = NR
            error_text = code
            allowed = armed
            armed = 0
            # count only the CALL'\''s parens — those from the macro name rightwards. A `);` belonging to
            # a nested call inside an argument no longer ends collection, and an argument that merely
            # ends in `)` no longer does either, because both balance out.
            depth = paren_delta(after_macro(code))
            if (depth > 0) {
                in_error_call = 1
            } else {
                # balanced (or paren-less) on its own line: complete, so decide it now
                if (!allowed) { check(error_text, error_start_line) }
                error_text = ""
            }
            next
        }

        # any other line of CODE clears a stale marker. A marker here — in a string literal, or on some
        # unrelated statement — is not the documented form and must not suppress the next call.
        armed = 0
    }

    # A call still being collected at end of file means its parens never balanced, i.e. this lexer
    # mis-read something (a raw string `r#"…"#` whose contents include a quote, a `'\''('\''` char
    # literal). The damage is not one missed call: everything after it in the file went unchecked too.
    # Report it, because the entire value of this check is that a clean run means something — silence
    # from a wedged collector is indistinguishable from a clean file, which is the one outcome it must
    # never produce.
    END {
        if (in_error_call) {
            print error_start_line ":UNPARSED: could not find where this call ends, so it and every " \
                  "call after it in this file went unchecked (report a linter bug)"
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
for dir in "${SEARCH_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        SCANNED_ANY=1
        while IFS= read -r file; do
            if ! check_file "$file"; then
                VIOLATIONS_FOUND=1
            fi
        done < <(find "$dir" -name "*.rs" -type f)
    fi
done

# A run that matched no directory scanned nothing and would otherwise print "check passed" — the most
# misleading outcome this script has, since it is indistinguishable from a clean codebase. Fail loudly:
# every caller either passes real directories or relies on the defaults, so an empty sweep is a bug in
# the invocation (a typo, a moved crate, a path that got word-split).
if [ "${SCANNED_ANY:-0}" -ne 1 ]; then
    echo -e "${RED}ERROR: none of the requested directories exist, so nothing was checked:${NC}"
    printf '  %s\n' "${SEARCH_DIRS[@]}"
    exit 1
fi

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
    echo "Or, if the error is captured inline in the match arm (e.g. \`Err(e)\` / \`Err(err)\`):"
    echo -e "  ${RED}tracing::warn!(\"...: {e}\");${NC}"
    echo "To:"
    echo -e "  ${GREEN}tracing::warn!(\"...: {e:#}\");${NC}"
    echo ""
    echo "If the binding only LOOKS like an error (most often a String carrying an"
    echo "already-rendered message), opt out on the line above, with a reason:"
    echo -e "  ${GREEN}// rcp-error-log-allow: <why {:#} would add nothing here>${NC}"
    echo ""
    echo "See CLAUDE.md for details."
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Error logging format check passed!${NC}"
exit 0
